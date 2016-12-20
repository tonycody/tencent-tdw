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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.lang.ClassNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.ParseTree;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.model.MIndexItem;
import org.apache.hadoop.hive.ql.dataImport.BaseDataExtract;
import org.apache.hadoop.hive.ql.dataImport.DBAndHiveCheck;
import org.apache.hadoop.hive.ql.dataImport.DBExternalTableUtil;
import org.apache.hadoop.hive.ql.dataImport.DataExtractFactory;
import org.apache.hadoop.hive.ql.dataImport.ExtractConfig;
import org.apache.hadoop.hive.ql.dataImport.fetchSize.IFetchSize;
import org.apache.hadoop.hive.ql.dataImport.fetchSize.MemoryAdaptiveFetchSize;
import org.apache.hadoop.hive.ql.dataToDB.BaseDBExternalDataLoad;
import org.apache.hadoop.hive.ql.dataToDB.LoadConfig;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.GBKIgnoreKeyOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.optimizer.GenMR4MapGBYRS;
import org.apache.hadoop.hive.ql.optimizer.MapJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.GenMRFileSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.GenMROperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink2;
import org.apache.hadoop.hive.ql.optimizer.GenMRTableScan1;
import org.apache.hadoop.hive.ql.optimizer.GenMRUnion1;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink3;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink4;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.LocalSelectWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.createViewDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.extractDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.indexInfoDesc;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.lateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.limitDesc;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.scriptDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;
import org.apache.hadoop.hive.ql.plan.udtfDesc;
import org.apache.hadoop.hive.ql.plan.unionDesc;
import org.apache.hadoop.hive.ql.plan.uniqueDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.sql.SQLTransfer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.util.TableUtil;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.apache.hadoop.hive.ql.plan.indexWork;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalOptimizer;
import StorageEngineClient.MyTextInputFormat;

import org.apache.hadoop.hive.ql.dataToDB.StoreAsPgdata;
import org.apache.hadoop.hive.ql.sqlToPg.SqlCondPushDown;
import org.apache.hadoop.hive.ql.PBJarTool;

public class SemanticAnalyzer extends BaseSemanticAnalyzer {

  private static final Log LOG = LogFactory
      .getLog("hive.ql.parse.SemanticAnalyzer");
  private boolean isDbExternalInsert = false;
  private HashMap<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner> aliasToPruner;
  private HashMap<TableScanOperator, exprNodeDesc> opToPartPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private LinkedHashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private HashMap<TableScanOperator, TablePartition> topToTable;
  private QB qb;
  private ASTNode ast;
  private int destTableId;
  private UnionProcContext uCtx;
  List<MapJoinOperator> listMapJoinOpsNoReducer;

  private static final String TEXTFILE_INPUT = TextInputFormat.class.getName();
  private static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class
      .getName();
  private static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class
      .getName();
  private static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class
      .getName();
  private static final String RCFILE_INPUT = RCFileInputFormat.class.getName();
  private static final String RCFILE_OUTPUT = RCFileOutputFormat.class
      .getName();
  private static final String FORMAT_INPUT = StorageEngineClient.FormatStorageInputFormat.class
      .getName();
  private static final String FORMAT_OUTPUT = StorageEngineClient.FormatStorageHiveOutputFormat.class
      .getName();
  private static final String FORMAT_SERDE = StorageEngineClient.FormatStorageSerDe.class
      .getName();
  private Map<String, String> dbDataTmpFilePathes = new HashMap<String, String>();
  private static final String PB_INPUT = "protobuf.mapred.ProtobufInputFormat";// "error";
  private static final String PB_OUTPUT = "protobuf.mapred.HiveProtobufOutputFormat";// "error";
  private static final String PB_SERDE = ProtobufSerDe.class.getName();

  private static final String PB_INPUT1 = "newprotobuf.mapred.ProtobufInputFormat";// "error";
  private static final String PB_OUTPUT1 = "newprotobuf.mapred.HiveProtobufOutputFormat";// "error";

  private boolean isStarAndAnalysisFunction = false;
  private Vector<String> rowsShouldnotbeShow = new Vector<String>();
  private boolean isFixUnionAndSelectStar = false;
  private boolean isFixUniqueUnionAndSelectStar = false;

  private boolean isAllDBExternalTable = true;
  private String pgTmpDir = "";
  private HashMap<String, Table> allTableMap = new HashMap<String, Table>();
  private Map<String, String> pgDataTmpFilePathes = new HashMap<String, String>();

  private HashMap<String, String> ipinfoTables = new HashMap<String, String>();
  private boolean ipinfoExist = false;

  private HashMap<String, Boolean> firstItemMap = new HashMap<String, Boolean>();

  private String pb_msg_outer_name = null;
  //public static final String protobufPackageName = "tdw";

  private static final String COLUMNAR_SERDE = ColumnarSerDe.class.getName();

  private Operator<? extends Serializable> filesinkop;

  private boolean hasWith = false;
  private boolean hasWithForParallel = false;
  private boolean hasUnionWith = false;
  private Map<String, ASTNode> withMap = null;
  private ArrayList<String> withQueries = null;
  private ArrayList<Boolean> beProcessed = null;
  private Map<String, Operator> withOps = null;
  private Map<String, Integer> withCounts = null;
  private ArrayList<QBExpr> withQBEs = null;
  private ArrayList<String> allTbls = null;

  private MultiHdfsInfo multiHdfsInfo = null;

  private Set<ReadEntity> inputs;

  private Set<WriteEntity> outputs;

  public boolean canUseIndex = true;
  public String filterExprString = "";

  public static class IndexValue {
    public String type = "";
    public Object value = null;
  }

  public static class IndexQueryInfo {
    public boolean isColumnMode = false;
    public boolean isIndexMode = false;
    public String fieldList = "";
    public String indexName = "";
    public String dbName = "";
    public String tblName = "";
    public Map<String, String> paramMap = null;
    public List<IndexValue> values = null;
    public List<String> partList = null;
    public String location = "";
    public int limit = -1;
    public int fieldNum = 0;
  }

  public IndexQueryInfo indexQueryInfo = new IndexQueryInfo();
  private List<FieldSchema> allColsList = null;
  private createViewDesc createVwDesc;
  private ArrayList<String> viewsExpanded;
  private ASTNode viewSelect;
  private static UnparseTranslator unparseTranslator;

  private static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {

    super(conf);
    this.aliasToPruner = new HashMap<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner>();
    this.opToPartPruner = new HashMap<TableScanOperator, exprNodeDesc>();
    this.aliasToSamplePruner = new HashMap<String, SamplePruner>();
    this.topOps = new LinkedHashMap<String, Operator<? extends Serializable>>();
    this.topSelOps = new HashMap<String, Operator<? extends Serializable>>();
    this.loadTableWork = new ArrayList<loadTableDesc>();
    this.loadFileWork = new ArrayList<loadFileDesc>();
    opParseCtx = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
    joinContext = new HashMap<JoinOperator, QBJoinTree>();
    topToTable = new HashMap<TableScanOperator, TablePartition>();
    this.destTableId = 1;
    this.uCtx = null;
    this.listMapJoinOpsNoReducer = new ArrayList<MapJoinOperator>();

    inputs = new LinkedHashSet<ReadEntity>();
    outputs = new LinkedHashSet<WriteEntity>();
    unparseTranslator = new UnparseTranslator();

    filesinkop = null;
    multiHdfsInfo = new MultiHdfsInfo(conf);
  }

  static boolean _innerMost_ = true;
  static boolean _plannerInnerMost_ = true;

  @Override
  protected void reset() {
    super.reset();
    this.aliasToPruner.clear();
    this.loadTableWork.clear();
    this.loadFileWork.clear();
    this.topOps.clear();
    this.topSelOps.clear();
    this.destTableId = 1;
    this.idToTableNameMap.clear();
    qb = null;
    ast = null;
    uCtx = null;
    this.aliasToSamplePruner.clear();
    this.joinContext.clear();
    this.opParseCtx.clear();

    _plannerInnerMost_ = true;
    _innerMost_ = true;
    _groupByKeys.clear();
    _distinctKeys.clear();
    _aliasMap = null;
  }

  public void init(ParseContext pctx) {
    aliasToPruner = pctx.getAliasToPruner();
    opToPartPruner = pctx.getOpToPartPruner();
    aliasToSamplePruner = pctx.getAliasToSamplePruner();
    topOps = pctx.getTopOps();
    topSelOps = pctx.getTopSelOps();
    opParseCtx = pctx.getOpParseCtx();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    joinContext = pctx.getJoinContext();
    ctx = pctx.getContext();
    destTableId = pctx.getDestTableId();
    idToTableNameMap = pctx.getIdToTableNameMap();
    this.uCtx = pctx.getUCtx();
    this.listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
    qb = pctx.getQB();
  }

  public ParseContext getParseContext() {
    return new ParseContext(conf, qb, ast, aliasToPruner, opToPartPruner,
        aliasToSamplePruner, topOps, topSelOps, opParseCtx, joinContext,
        topToTable, loadTableWork, loadFileWork, ctx, idToTableNameMap,
        destTableId, uCtx, listMapJoinOpsNoReducer);
  }

  @SuppressWarnings("nls")
  public void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias)
      throws SemanticException {

    assert (ast.getToken() != null);
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_QUERY: {
      QB qb = new QB(id, alias, true);
      doPhase1(ast, qb, initPhase1Ctx());
      qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
      qbexpr.setQB(qb);
    }
      break;
    case HiveParser.TOK_UNION: {
      qbexpr.setOpcode(QBExpr.Opcode.UNION);
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
      doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id + "-subquery1",
          alias + "-subquery1");
      qbexpr.setQBExpr1(qbexpr1);

      assert (ast.getChild(1) != null);
      QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
      doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id + "-subquery2",
          alias + "-subquery2");
      qbexpr.setQBExpr2(qbexpr2);
    }
      break;
    case HiveParser.TOK_UNIQUE_UNION: {
      qbexpr.setOpcode(QBExpr.Opcode.UNIQUE_UNION);
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
      doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id + "-subquery1",
          alias + "-subquery1");
      qbexpr.setQBExpr1(qbexpr1);

      assert (ast.getChild(1) != null);
      QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
      doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id + "-subquery2",
          alias + "-subquery2");
      qbexpr.setQBExpr2(qbexpr2);
    }
      break;
    }
  }

  private LinkedHashMap<String, ASTNode> doPhase1GetAggregationsFromSelect(
      ASTNode selExpr) {
    LinkedHashMap<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode sel = (ASTNode) selExpr.getChild(i).getChild(0);
      doPhase1GetAllAggregations(sel, aggregationTrees);
    }
    return aggregationTrees;
  }

  private LinkedHashMap<String, ASTNode> doPhase1GetAnalysisesFromSelect(
      ASTNode selExpr) throws SemanticException {
    LinkedHashMap<String, ASTNode> analysisTrees = new LinkedHashMap<String, ASTNode>();

    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode sel = (ASTNode) selExpr.getChild(i).getChild(0);
      doPhase1GetAllAnalysises(sel, analysisTrees);
    }

    return analysisTrees;
  }

  private void doPhase1GetAllAnalysises(ASTNode expressionTree,
      HashMap<String, ASTNode> analysises) throws SemanticException {
    String funcName = null;

    if (expressionTree.getToken().getType() == HiveParser.TOK_FUNCTIONOVER) {
      funcName = ((ASTNode) (expressionTree.getChild(0))).getText();
      ((ASTNode) (expressionTree.getChild(0))).getToken().setText(
          funcName + "over");

      if (funcName.equalsIgnoreCase("LAG") || funcName.equalsIgnoreCase("LEAD")
          || funcName.equalsIgnoreCase("RANK")
          || funcName.equalsIgnoreCase("DENSE_RANK")
          || funcName.equalsIgnoreCase("ROW_NUMBER")) {
        if (expressionTree.getChild(1) == null) {
          throw new SemanticException(
              "order by clauses do not exist in the Analysis Functions "
                  + funcName);
        }
      }
    }

    if (expressionTree.getChild(1) != null) {
      if (expressionTree.getToken().getType() == HiveParser.TOK_FUNCTIONOVER
          && ((ASTNode) expressionTree.getChild(1)).getText().equalsIgnoreCase(
              "distinct")) {
        if (funcName.equalsIgnoreCase("LAG")
            || funcName.equalsIgnoreCase("LEAD")
            || funcName.equalsIgnoreCase("RANK")
            || funcName.equalsIgnoreCase("DENSE_RANK")
            || funcName.equalsIgnoreCase("ROW_NUMBER")
            || funcName.equalsIgnoreCase("RATIO_TO_REPORT"))
          throw new SemanticException("The analysis function "
              + funcName.toUpperCase() + " is not allowed to use distinct!");

        expressionTree.getToken().setType(HiveParser.TOK_FUNCTIONOVERDI);
        expressionTree.getToken().setText("TOK_FUNCTIONOVERDI");
        expressionTree.deleteChild(1);
      }
    }

    int exprTokenType = expressionTree.getToken().getType();

    if (exprTokenType == HiveParser.TOK_FUNCTIONOVER
        || exprTokenType == HiveParser.TOK_FUNCTIONOVERDI) {
      assert (expressionTree.getChildCount() != 0);

      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());

        if (FunctionRegistry.getGenericUDWFResolver(functionName) != null) {
          analysises.put(expressionTree.toStringTree(), expressionTree);
          return;
        }
      }
    }

    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAnalysises((ASTNode) expressionTree.getChild(i), analysises);
    }
  }

  private void doPhase1GetColumnAliasesFromSelect(ASTNode selectExpr,
      QBParseInfo qbp) {
    for (int i = 0; i < selectExpr.getChildCount(); ++i) {
      ASTNode selExpr = (ASTNode) selectExpr.getChild(i);
      if ((selExpr.getToken().getType() == HiveParser.TOK_SELEXPR)
          && (selExpr.getChildCount() == 2)) {
        String columnAlias = unescapeIdentifier(selExpr.getChild(1).getText());
        qbp.setExprToColumnAlias((ASTNode) selExpr.getChild(0), columnAlias);
      }
    }
  }

  private void doPhase1GetAllAggregations(ASTNode expressionTree,
      HashMap<String, ASTNode> aggregations) {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          aggregations.put(expressionTree.toStringTree(), expressionTree);
          FunctionInfo fi = FunctionRegistry.getFunctionInfo(functionName);
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expressionTree
                .getChild(0));
          }
          return;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(i),
          aggregations);
    }
  }

  static ArrayList<ToolBox.tableTuple> _groupByKeys = new ArrayList<ToolBox.tableTuple>();
  static int indent = 0;

  private static String _analyzeGroupByASTNode(ASTNode _para) {

    for (Node _node : _para.getChildren()) {
      ASTNode _astNode = (ASTNode) _node;
      if (_astNode.getName().equalsIgnoreCase(String.valueOf(HiveParser.DOT))) {
        assert (_astNode.getChildren().size() == 2);
        indent++;
        String _tableName_ = _analyzeGroupByASTNode((ASTNode) _astNode
            .getChildren().get(0));
        indent--;
        String _fieldName_ = ((ASTNode) _astNode.getChildren().get(1))
            .getText();
        ToolBox.tableTuple _tt = new ToolBox.tableTuple(_tableName_,
            _fieldName_);

        if (_plannerInnerMost_) {
          _groupByKeys.add(_tt);
        }

      } else if (_astNode.getName().equalsIgnoreCase(
          String.valueOf(HiveParser.TOK_TABLE_OR_COL))) {
        for (Node _childNode : _node.getChildren()) {
          if (((ASTNode) _childNode).getName().equalsIgnoreCase(
              String.valueOf(HiveParser.DOT))) {
            indent++;
            _analyzeGroupByASTNode((ASTNode) _childNode);
            indent--;
          } else {
            String _fieldName_ = ((ASTNode) _childNode).getText();
            ToolBox.tableTuple _tt = new ToolBox.tableTuple("", _fieldName_);
            if (_plannerInnerMost_) {
              _groupByKeys.add(_tt);
            }
          }

        }
      } else if (_astNode.getName().equalsIgnoreCase(
          String.valueOf(HiveParser.Identifier))) {
        return unescapeIdentifier(_astNode.getText()).toLowerCase();

      }

    }

    return null;
  }

  static ToolBox.tableDistinctTuple _tdt = null;
  static ArrayList<ToolBox.tableDistinctTuple> _distinctKeys = new ArrayList<ToolBox.tableDistinctTuple>();

  private static void _analyzeDistinctByASTNode(ASTNode _para) {

    ASTNode _countAstNode = (ASTNode) _para.getChildren().get(0);

    ASTNode _dotAstNode = (ASTNode) _para.getChildren().get(1);
    if (_dotAstNode.getChildren().size() == 2) {
      if (_dotAstNode.getText().equals("+")) {
        return;
      } else if (_dotAstNode.getText().equals(".")) {
        ASTNode _aliasAstNode = (ASTNode) _dotAstNode.getChildren().get(0)
            .getChildren().get(0);
        ASTNode _fieldAstNode = (ASTNode) _dotAstNode.getChildren().get(1);
        _tdt = new ToolBox.tableDistinctTuple(_aliasAstNode.getText(),
            _fieldAstNode.getText());
      }
    } else {
      ASTNode _ = (ASTNode) _dotAstNode.getChildren().get(0);
      _tdt = new ToolBox.tableDistinctTuple("", _.getText());
    }

    if (_plannerInnerMost_) {
      if (_distinctKeys == null) {
        _distinctKeys = new ArrayList<ToolBox.tableDistinctTuple>();
        LOG.error("_distinctKeys is null !!!!");
      }
      _distinctKeys.add(_tdt);
    }

  }

  private List<ASTNode> doPhase1GetDistinctFuncExpr(
      HashMap<String, ASTNode> aggregationTrees) throws SemanticException {
    List<ASTNode> exprs = new ArrayList<ASTNode>();
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      assert (value != null);
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
        _analyzeDistinctByASTNode(value);
        exprs.add(value);
      }
    }
    return exprs;
  }

  private List<ASTNode> doPhase1GetDistinctFuncExprFromAnalysisExprs(
      HashMap<String, ASTNode> analysisTrees) throws SemanticException {
    List<ASTNode> exprs = null;

    for (Map.Entry<String, ASTNode> entry : analysisTrees.entrySet()) {
      ASTNode value = entry.getValue();
      assert (value != null);

      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONOVERDI) {
        if (exprs == null) {
          exprs = new ArrayList<ASTNode>();
        }
        exprs.add(value);
      }
    }

    return exprs;
  }

  private void doPhase1JudgeAnalysises(HashMap<String, ASTNode> analysisTrees,
      List<ASTNode> distTrees) throws SemanticException {
    boolean containDist = false;
    boolean containOrderBy = false;
    ArrayList<String> partitionByNodes = null;
    int partitionByCount = -1;
    ArrayList<String> orderByNodes = null;
    ArrayList<String> orderByWithoutDirec = null;
    int orderByCount = -1;
    ASTNode orderByTree = null;

    if (distTrees != null) {
      containDist = true;
    }
    ArrayList<Boolean> hasPartitionBys = new ArrayList<Boolean>();
    for (int i = 0; i < analysisTrees.values().size(); i++) {
      hasPartitionBys.add(false);
    }
    int count = -1;

    boolean hasPartitionBy = false;
    for (ASTNode analysisTree : analysisTrees.values()) {
      count++;
      int childCount = analysisTree.getChildCount();
      if (childCount > 4)
        throw new SemanticException(
            ErrorMsg.PARAMETER_NUMBER_NOT_MATCH.getMsg(analysisTree.getChild(0)
                .getText().toUpperCase()));

      for (int i = 0; i < childCount; i++) {
        if (analysisTree.getChild(i).getType() == HiveParser.TOK_PARTITIONBY) {
          hasPartitionBy = true;
          hasPartitionBys.set(count, true);
          if (partitionByNodes == null) {
            ASTNode partitionBy = (ASTNode) analysisTree.getChild(i);

            partitionByNodes = new ArrayList<String>();
            partitionByCount = analysisTree.getChild(i).getChildCount();
            for (int j = 0; j < partitionByCount; j++) {
              String par = ((ASTNode) analysisTree.getChild(i)).getChild(j)
                  .toStringTree().toLowerCase();
              if (partitionByNodes.contains(par))
                throw new SemanticException(
                    "Some partition by Columns are the same!");

              if ((orderByWithoutDirec != null)
                  && (orderByWithoutDirec.contains(par)))
                throw new SemanticException(
                    "Some order by Columns are the same with partition by Columns in analysis functions!");

              partitionByNodes.add(((ASTNode) analysisTree.getChild(i))
                  .getChild(j).toStringTree().toLowerCase());
            }
            break;
          } else {
            int childcount = analysisTree.getChild(i).getChildCount();
            if (childcount != partitionByCount) {
              throw new SemanticException(
                  ErrorMsg.PARTITION_BY_PARAMETER_NOT_MATCH.getMsg(analysisTree
                      .getChild(0).getText().toUpperCase()));
            }

            for (int j = 0; j < childcount; j++) {
              if (!partitionByNodes.contains(((ASTNode) analysisTree
                  .getChild(i)).getChild(j).toStringTree().toLowerCase()))
                throw new SemanticException(
                    ErrorMsg.PARTITION_BY_PARAMETER_NOT_MATCH
                        .getMsg(analysisTree.getChild(0).getText()
                            .toUpperCase()));
            }

          }

        }
      }

      boolean hasOrderBy = false;
      for (int i = 0; i < childCount; i++) {
        if (analysisTree.getChild(i).getType() == HiveParser.TOK_ORDERBY) {
          if (orderByNodes == null) {
            orderByNodes = new ArrayList<String>();
            orderByWithoutDirec = new ArrayList<String>();
            orderByCount = analysisTree.getChild(i).getChildCount();
            for (int j = 0; j < orderByCount; j++) {
              String withoutDirec = ((ASTNode) (((ASTNode) analysisTree
                  .getChild(i)).getChild(j)).getChild(0)).toStringTree()
                  .toLowerCase();
              if (orderByWithoutDirec.contains(withoutDirec)) {
                throw new SemanticException(
                    "Some order by Columns are the same!");
              }
              if (hasPartitionBy && (partitionByNodes.contains(withoutDirec)))
                throw new SemanticException(
                    "Some order by Columns are the same with partition by Columns in analysis functions!");
              orderByWithoutDirec.add(withoutDirec);
              orderByNodes.add(((ASTNode) analysisTree.getChild(i)).getChild(j)
                  .toStringTree().toLowerCase());
            }

            hasOrderBy = true;
            break;
          } else {
            int childcount = analysisTree.getChild(i).getChildCount();
            if (childcount != orderByCount) {
              throw new SemanticException(
                  ErrorMsg.ORDER_BY_PARAMETER_NOT_MATCH.getMsg(analysisTree
                      .getChild(0).getText().toUpperCase()));
            }

            for (int j = 0; j < childcount; j++) {
              if (!orderByNodes.contains(((ASTNode) analysisTree.getChild(i))
                  .getChild(j).toStringTree().toLowerCase()))
                throw new SemanticException(
                    ErrorMsg.ORDER_BY_PARAMETER_NOT_MATCH.getMsg(analysisTree
                        .getChild(0).getText().toUpperCase()));
            }

          }

          hasOrderBy = true;
        }
      }
      if (!hasOrderBy
          && (analysisTree.getChild(0).getText().equalsIgnoreCase("LAGOVER")
              || analysisTree.getChild(0).getText()
                  .equalsIgnoreCase("LEADOVER")
              || analysisTree.getChild(0).getText()
                  .equalsIgnoreCase("RANKOVER")
              || analysisTree.getChild(0).getText()
                  .equalsIgnoreCase("DENSE_RANKOVER")
              || analysisTree.getChild(0).getText()
                  .equalsIgnoreCase("FIRST_VALUEOVER")
              || analysisTree.getChild(0).getText()
                  .equalsIgnoreCase("LAST_VALUEOVER") || analysisTree
              .getChild(0).getText().equalsIgnoreCase("ROW_NUMBEROVER")))
        throw new SemanticException(
            ErrorMsg.ORDER_BY_CLAUSE_NOT_EXIST.getMsg(analysisTree.getChild(0)
                .getText().toUpperCase()));

      if (analysisTree.getChild(0).getText().equalsIgnoreCase("RANKOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("DENSE_RANKOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("ROW_NUMBEROVER")) {

        if (((ASTNode) (analysisTree.getChild(1))).getToken().getType() == HiveParser.TOK_FUNCPARAMETER)
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should not have parameters!");
      }

      if (analysisTree.getChild(0).getText().equalsIgnoreCase("LAGOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("LEADOVER")) {

        ASTNode parameters = (ASTNode) analysisTree.getChild(1);
        if (parameters.getToken().getType() != HiveParser.TOK_FUNCPARAMETER)
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should have parameters!");
        else {
          if (parameters.getChildCount() < 1 || parameters.getChildCount() > 3) {
            throw new SemanticException("The analysis function "
                + analysisTree.getChild(0).getText().toUpperCase()
                + " should have 1-3 parameters!");
          }
        }
      }

      if (analysisTree.getChild(0).getText().equalsIgnoreCase("SUMOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("AVGOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("MAXOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("MINOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("FIRST_VALUEOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("LAST_VALUEOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("RATIO_TO_REPORTOVER")) {

        ASTNode parameters = (ASTNode) analysisTree.getChild(1);
        if (parameters == null) {
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should have parameters!");
        }
        if (parameters.getToken().getType() != HiveParser.TOK_FUNCPARAMETER)
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should have parameters!");
        else {
          if (parameters.getChildCount() != 1) {
            throw new SemanticException("The analysis function "
                + analysisTree.getChild(0).getText().toUpperCase()
                + " should have 1 parameter!");
          }
        }
      }

      if (analysisTree.getChild(0).getText().equalsIgnoreCase("SUMOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("COUNTOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("AVGOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("MAXOVER")
          || analysisTree.getChild(0).getText().equalsIgnoreCase("MINOVER")
          || analysisTree.getChild(0).getText()
              .equalsIgnoreCase("RATIO_TO_REPORTOVER")) {
      }

      if (analysisTree.getChild(0).getText().equalsIgnoreCase("COUNTOVER")) {
        ASTNode parameters = (ASTNode) analysisTree.getChild(1);
        if (parameters == null) {
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should have parameters!");
        }
        if (parameters.getToken().getType() != HiveParser.TOK_FUNCPARAMETER)
          throw new SemanticException("The analysis function "
              + analysisTree.getChild(0).getText().toUpperCase()
              + " should have parameters!");
        else {
          if (parameters.getChildCount() != 1) {
            throw new SemanticException("The analysis function "
                + analysisTree.getChild(0).getText().toUpperCase()
                + " should have 1 parameter!");
          } else {
            ASTNode parameter = (ASTNode) parameters.getChild(0);
            if (parameter.getText().equals("1")) {
              analysisTree.deleteChild(1);
            }
          }
        }
      }
    }

    if (hasPartitionBys.size() > 0) {
      boolean hasP = hasPartitionBys.get(0);
      for (boolean one : hasPartitionBys) {
        if (one != hasP)
          throw new SemanticException(
              "All the Analysis Functions must have or have no Partition Bys at the same time!");
      }
    }

    if (containDist == true && orderByCount != -1)
      throw new SemanticException(
          ErrorMsg.ORDER_BY_CONFLICT_WITH_DISTINCT.getMsg());

    String dist = null;
    if (containDist) {
      for (ASTNode distTree : distTrees) {
        if (dist == null)
          dist = distTree.getChild(1).toStringTree();
        else {
          if (!dist.equalsIgnoreCase(distTree.getChild(1).toStringTree()))
            throw new SemanticException(
                "Only the same distinct column is allowed in Analysis Functions!");
        }

      }
    }

  }

  ToolBox.tableAliasTuple _tableAliasTuple_ = null;

  private String processTable(QB qb, ASTNode tabref) throws SemanticException {
    boolean tableSamplePresent = false;
    int aliasIndex = 0;
    if (tabref.getChildCount() == 2) {
      ASTNode ct = (ASTNode) tabref.getChild(1);
      if (ct.getToken().getType() == HiveParser.TOK_TABLESAMPLE) {
        tableSamplePresent = true;
      } else {
        aliasIndex = 1;
      }
    } else if (tabref.getChildCount() == 3) {
      aliasIndex = 2;
      tableSamplePresent = true;
    }
    ASTNode tableTree = (ASTNode) (tabref.getChild(0));
    String UserAlias = null;

    if (aliasIndex == 0) {
      ;
    } else
      UserAlias = unescapeIdentifier(tabref.getChild(aliasIndex).getText())
          .toLowerCase();

    if (UserAlias != null && qb.existsUserAlias(UserAlias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref
          .getChild(aliasIndex)));
    }

    String table_name = unescapeIdentifier(
        tabref.getChild(0).getChild(0).getText()).toLowerCase();

    boolean isWithQuery = false;
    if (this.hasWith && this.withQueries.contains(table_name)) {
      throw new SemanticException(
          "some table name conflicts with a subquery in with: " + table_name);
    }
    if (this.allTbls != null) {
      if (table_name != null)
        this.allTbls.add(table_name.toLowerCase());
      if (UserAlias != null)
        this.allTbls.add(UserAlias.toLowerCase());
    }

    String db = null;
    String partitionRef = null;
    String subPartititonRef = null;
    QB.PartRefType prt = null;

    if (tabref.getChild(0).getChildCount() >= 2) {
      if (isWithQuery)
        throw new SemanticException("With queries do not support partitions!");
      LOG.debug("tabref' stringTree :" + tabref.toStringTree());

      switch (((ASTNode) tabref.getChild(0).getChild(1)).getToken().getType()) {
      case HiveParser.TOK_PARTITIONREF:
        partitionRef = unescapeIdentifier(
            tabref.getChild(0).getChild(1).getChild(0).getText()).toLowerCase();
        prt = QB.PartRefType.PRI;
        break;
      case HiveParser.TOK_SUBPARTITIONREF:
        subPartititonRef = unescapeIdentifier(
            tabref.getChild(0).getChild(1).getChild(0).getText()).toLowerCase();
        prt = QB.PartRefType.SUB;
        break;
      case HiveParser.TOK_COMPPARTITIONREF:
        partitionRef = unescapeIdentifier(
            tabref.getChild(0).getChild(1).getChild(0).getText()).toLowerCase();
        subPartititonRef = unescapeIdentifier(
            tabref.getChild(0).getChild(1).getChild(1).getText()).toLowerCase();
        prt = QB.PartRefType.COMP;
        break;
      default:
        db = unescapeIdentifier(tabref.getChild(0).getChild(1).getText())
            .toLowerCase();

      }
      if (tabref.getChild(0).getChildCount() == 3) {
        db = unescapeIdentifier(tabref.getChild(0).getChild(2).getText())
            .toLowerCase();
      }

    }

    if (aliasIndex == 0 && prt != null) {
      throw new SemanticException("table partition must specific a alias!");
    }

    if (db == null) {
      db = SessionState.get().getDbName();
    }

    String alias = null;
    if (!isWithQuery) {
      if (UserAlias != null) {
        qb.putDBTB2UserAlias(db + "/" + table_name, UserAlias);
        alias = db + "/" + table_name + "#" + UserAlias;
        LOG.debug("put table : " + db + "/" + table_name + ",and alias : "
            + alias);
        qb.setTabAlias(alias, new QB.tableRef(db, table_name, prt,
            partitionRef, subPartititonRef));
        qb.getParseInfo().setSrcForAlias(alias, tableTree);
        qb.setUserAliasToTabs(UserAlias, qb.getTableRef(alias));

      } else {
        alias = db + "/" + table_name;
        qb.setTabAlias(alias, new QB.tableRef(db, table_name, prt,
            partitionRef, subPartititonRef));
        qb.getParseInfo().setSrcForAlias(alias, tableTree);
        LOG.debug("put table : " + db + "/" + table_name + ",and alias : "
            + alias);
      }
    } else {
      if (UserAlias != null) {
        qb.putWith2UserAlias(table_name, UserAlias);
        qb.setWithAlias(UserAlias, table_name);
        qb.getParseInfo().setSrcForAlias(UserAlias, withMap.get(table_name));
      } else {
        alias = table_name;
        qb.setWithAlias(alias, table_name);
        qb.getParseInfo().setSrcForAlias(alias, withMap.get(table_name));
      }
      qb.putWith(table_name);
    }

    if (!isWithQuery)
      qb.putDBTB(db + "/" + table_name);

    if (!isWithQuery && tableSamplePresent) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(1);
      ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((ASTNode) sampleClause.getChild(i));
        }
      }
      if (sampleCols.size() > 2) {
        throw new SemanticException(ErrorMsg.SAMPLE_RESTRICTION.getMsg(tabref
            .getChild(0)));
      }
      qb.getParseInfo()
          .setTabSample(
              alias,
              new TableSample(unescapeIdentifier(
                  sampleClause.getChild(0).getText()).toLowerCase(),
                  unescapeIdentifier(sampleClause.getChild(1).getText())
                      .toLowerCase(), sampleCols));
      if (unparseTranslator.isEnabled()) {
        for (ASTNode sampleCol : sampleCols) {
          unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
              .getChild(0));
        }
      }
    }

    _tableAliasTuple_ = new ToolBox.tableAliasTuple(table_name, alias);

    if (!isWithQuery) {
      if (_aliasMap == null) {
        _aliasMap = new HashMap<String, HashSet<String>>();
        HashSet<String> _reg_ = new HashSet<String>();
        _reg_.add(alias);
        _aliasMap.put(table_name, _reg_);
      } else {
        HashSet<String> _reg_ = _aliasMap.get(table_name);
        if (_reg_ == null) {
          _reg_ = new HashSet<String>();
        }
        _reg_.add(alias);
        _aliasMap.put(table_name, _reg_);
      }
    }

    if (!isWithQuery) {
      qb.setTabAlias(alias, new QB.tableRef(db, table_name, prt, partitionRef,
          subPartititonRef));

      qb.getParseInfo().setSrcForAlias(alias, tableTree);

      unparseTranslator.addIdentifierTranslation(
          (ASTNode) tableTree.getChild(0), db);
      if (aliasIndex != 0) {
        unparseTranslator.addIdentifierTranslation((ASTNode) tabref
            .getChild(aliasIndex));
      }

    }

    return alias;
  }

  void checkAlias(QBExpr qbexpr, String alias) throws SemanticException {
    if (qbexpr == null || alias == null) {
      return;
    }
    if (qbexpr.getQB() == null) {
      return;
    }
    if (qbexpr.getQB().getSubqForAlias(alias) != null) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(alias));
    }
    for (String str : qbexpr.getQB().getSubqAliases()) {
      checkAlias(qbexpr.getQB().getSubqForAlias(str), alias);
    }
  }

  private String processSubQuery(QB qb, ASTNode subq) throws SemanticException {

    String alias;
    ASTNode subqref = (ASTNode) subq.getChild(0);
    if (subq.getChildCount() == 1) {
      Random r = new Random();
      alias = "tmprandomtbl_" + r.nextInt(10000);
      while (qb.exists(alias)) {
        alias = "tmprandomtbl_" + r.nextInt(10000);
      }
      subq.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, alias)));

      ASTNode aliasNode = (ASTNode) subq.getChild(1);
      if (aliasNode != null) {
        aliasNode.setTokenStartIndex(subq.getTokenStopIndex() + 1);
        aliasNode.setTokenStopIndex(subq.getTokenStopIndex());
      }
      unparseTranslator.addIdentifierInsertTranslation((ASTNode) subq
          .getChild(1));

    } else {
      alias = unescapeIdentifier(subq.getChild(1).getText()).toLowerCase();
    }

    if (this.allTbls != null && alias != null)
      this.allTbls.add(alias.toLowerCase());
    if (this.hasWith && this.withQueries != null
        && this.withQueries.contains(alias))
      throw new SemanticException("Conflict with sub-queries in with: " + alias);

    QBExpr qbexpr = new QBExpr(alias);

    doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias);

    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq
          .getChild(1)));
    }

    qb.setSubqAlias(alias, qbexpr);

    unparseTranslator.addIdentifierTranslation((ASTNode) subq.getChild(1));

    return alias;
  }

  private String processSubQuery(QB qb, ASTNode subq, int idx)
      throws SemanticException {

    String alias;
    ASTNode subqref = subq;
    assert (subq.getChildCount() == 2);
    alias = this.withQueries.get(idx);

    QBExpr qbexpr = null;
    if (!this.beProcessed.get(idx)) {
      qbexpr = new QBExpr(alias);

      doPhase1QBExpr(subqref, qbexpr, null, alias);

      this.withQBEs.set(idx, qbexpr);
      this.beProcessed.set(idx, true);
    } else
      qbexpr = this.withQBEs.get(idx);

    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq
          .getChild(1)));
    }
    qb.setSubqAlias(alias, qbexpr);
    if (this.allTbls != null && alias != null
        && !this.withQueries.contains(alias))
      this.allTbls.add(alias);

    unparseTranslator.addIdentifierTranslation(new ASTNode(new CommonToken(
        HiveParser.Identifier, alias)));

    return alias;
  }

  private boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN))
      return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void processJoin(QB qb, ASTNode join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3)
        && join.getToken().getType() != HiveParser.TOK_UNIQUEJOIN)
      throw new SemanticException("Join with multiple children");

    for (int num = 0; num < numChildren; num++) {
      ASTNode child = (ASTNode) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF) {
        String tab = child.getChild(0).getChild(0).getText().toLowerCase();
        if (!this.hasWith || child.getChild(0).getChildCount() > 1
            || !this.withQueries.contains(tab))
          processTable(qb, child);
        else {
          if (this.allTbls != null && this.allTbls.contains(tab))
            throw new SemanticException(
                "a subquery in with conflicts with some table name");
          int idx = this.withQueries.indexOf(tab);
          this.withCounts.put(tab, this.withCounts.get(tab) + 1);
          processSubQuery(qb, this.withMap.get(tab), idx);
        }
      } else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        processSubQuery(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
        throw new SemanticException(
            ErrorMsg.LATERAL_VIEW_WITH_JOIN.getMsg(join));
      } else if (isJoinToken(child)) {
        processJoin(qb, child);
      }
    }
  }

  private String processLateralView(QB qb, ASTNode lateralView)
      throws SemanticException {
    int numChildren = lateralView.getChildCount();

    assert (numChildren == 2);
    ASTNode next = (ASTNode) lateralView.getChild(1);

    String alias = null;

    switch (next.getToken().getType()) {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, next);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, next);
      break;
    case HiveParser.TOK_LATERAL_VIEW:
      alias = processLateralView(qb, next);
      break;
    default:
      throw new SemanticException(
          ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(lateralView));
    }
    qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
    if (this.allTbls != null && alias != null)
      this.allTbls.add(alias);
    return alias;
  }

  private void chBetweenInAstAll(ASTNode ast) {
    for (int i = 0; i < ast.getChildCount(); i++) {
      chBetweenInAst(ast, (ASTNode) ast.getChild(i), i);
    }
  }

  private void chBetweenInAst(ASTNode father, ASTNode child, int index) {
    if (child.getToken().getType() == HiveParser.KW_BETWEEN) {
      ASTNode t0 = new ASTNode(new CommonToken(HiveParser.KW_AND, "and"));
      ASTNode t1 = new ASTNode(new CommonToken(HiveParser.GREATERTHANOREQUALTO,
          ">="));
      t1.addChild((ASTNode) child.getChild(0));
      t1.addChild((ASTNode) child.getChild(1));
      ASTNode t2 = new ASTNode(new CommonToken(HiveParser.LESSTHANOREQUALTO,
          "<="));
      t2.addChild(((ASTNode) child.getChild(0)).repNode());
      t2.addChild((ASTNode) child.getChild(2));
      t0.addChild(t1);
      t0.addChild(t2);
      father.setChild(index, t0);
    } else {
      if (child.getChildCount() == 0) {
        return;
      }
      for (int i = 0; i < child.getChildCount(); i++) {
        chBetweenInAst(child, (ASTNode) child.getChild(i), i);
      }
    }
  }

  void chwithtreeoffrom(LinkedHashMap<String, ASTNode> tree, ASTNode ast)
      throws SemanticException {
    LOG.debug("chwithtreeoffrom:  " + ast.dump());
    if (ast.getChildCount() >= 2) {
      ASTNode ch1 = (ASTNode) ast.getChild(0);
      ASTNode ch2 = (ASTNode) ast.getChild(1);
      if (ch1.getToken().getType() == HiveParser.TOK_TABREF) {
        ASTNode chch1 = (ASTNode) ch1.getChild(0);
        if (chch1.getToken().getType() == HiveParser.TOK_TAB) {
          String tname = chch1.getChild(0).getText().toLowerCase();
          ASTNode newtname = tree.get(tname);
          String tnamealias = null;
          if (ch1.getChildCount() == 2) {
            tnamealias = ch1.getChild(1).getText().toLowerCase();
          }
          LOG.debug(tname);
          LOG.debug("alias: " + tnamealias);
          if (tnamealias != null && newtname != null) {
            if (newtname.getChildCount() == 2) {
              newtname.setChild(1, new ASTNode(new CommonToken(
                  HiveParser.Identifier, tnamealias)));
            }
          }
          if (newtname != null) {
            ast.setChild(0, newtname);
          }
        }
      } else {
        chwithtreeoffrom(tree, ch1);
      }
      if (ch2.getToken().getType() == HiveParser.TOK_TABREF) {
        ASTNode chch2 = (ASTNode) ch2.getChild(0);
        if (chch2.getToken().getType() == HiveParser.TOK_TAB) {
          String tname = chch2.getChild(0).getText().toLowerCase();
          ASTNode newtname = tree.get(tname);
          String tnamealias = null;
          if (ch2.getChildCount() == 2) {
            tnamealias = ch2.getChild(1).getText().toLowerCase();
          }
          LOG.debug(tname);
          LOG.debug("alias: " + tnamealias);
          if (tnamealias != null && newtname != null) {
            if (newtname.getChildCount() == 2) {
              newtname.setChild(1, new ASTNode(new CommonToken(
                  HiveParser.Identifier, tnamealias)));
            }
          }
          if (newtname != null) {
            ast.setChild(1, newtname);
          }
        }
      } else {
        chwithtreeoffrom(tree, ch2);
      }
    } else if (ast.getChildCount() == 1) {
      ASTNode ch1 = (ASTNode) ast.getChild(0);
      if (ch1.getToken().getType() == HiveParser.TOK_TABREF) {
        ASTNode chch1 = (ASTNode) ch1.getChild(0);
        if (chch1.getToken().getType() == HiveParser.TOK_TAB) {
          String tname = chch1.getChild(0).getText().toLowerCase();
          String tnamealias = null;
          if (ch1.getChildCount() == 2) {
            tnamealias = ch1.getChild(1).getText().toLowerCase();
          }
          LOG.debug(tname);
          LOG.debug("alias: " + tnamealias);
          ASTNode newtname = tree.get(tname);
          if (tnamealias != null && newtname != null) {
            if (newtname.getChildCount() == 2) {
              newtname.setChild(1, new ASTNode(new CommonToken(
                  HiveParser.Identifier, tnamealias)));
            }
          }
          if (newtname != null) {
            ast.setChild(0, newtname);
          }
        }
      } else {
        chwithtreeoffrom(tree, ch1);
      }
    }

  }

  void chwithtree(LinkedHashMap<String, ASTNode> tree, ASTNode ast)
      throws SemanticException {
    ASTNode ch = (ASTNode) ast.getChild(0);
    if (ch.getToken().getType() == HiveParser.TOK_TABREF) {
      ASTNode chch = (ASTNode) ch.getChild(0);
      if (chch.getToken().getType() == HiveParser.TOK_TAB) {
        String tname = chch.getChild(0).getText().toLowerCase();

        String tnamealias = null;
        if (ch.getChildCount() == 2) {
          tnamealias = ch.getChild(1).getText().toLowerCase();
        }
        
        ASTNode newtname = tree.get(tname);
        if (tnamealias != null && newtname != null) {
          if (newtname.getChildCount() == 2) {
            newtname.setChild(1, new ASTNode(new CommonToken(
                HiveParser.Identifier, tnamealias)));
          }
        }
        if (newtname != null) {
          ASTNode useless = (ASTNode) ast.deleteChild(0);
          ast.addChild(newtname);
        }
      }
    } else if (ch.getToken().getType() == HiveParser.TOK_JOIN
        || ch.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
        || ch.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
        || ch.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
        || ch.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
      chwithtreeoffrom(tree, ch);
    } else if (ch.getToken().getType() == HiveParser.TOK_SUBQUERY) {
      dealWithSubQ(tree, ch);
    } else if (ch.getToken().getType() == HiveParser.TOK_QUERY) {
      dealWithSubQ(tree, ch);
    }
  }

  void dealWithSubQRecur(LinkedHashMap<String, ASTNode> tree,
      ASTNode subastchild, int childcount) throws SemanticException {
    if (subastchild.getToken().getType() == HiveParser.TOK_FROM) {
      chwithtree(tree, subastchild);
    } else {
      if (subastchild.getChildCount() == 0) {
        return;
      }
      for (int i = 0; i < subastchild.getChildCount(); i++) {
        dealWithSubQRecur(tree, (ASTNode) subastchild.getChild(i), i);
      }
    }
  }

  void dealWithSubQ(LinkedHashMap<String, ASTNode> tree, ASTNode sub)
      throws SemanticException {
    for (int i = 0; i < sub.getChildCount(); i++) {
      dealWithSubQRecur(tree, (ASTNode) sub.getChild(i), i);
    }
  }

  ASTNode dealwithsql(LinkedHashMap<String, ASTNode> tree, ASTNode astt)
      throws SemanticException {

    ASTNode withAst = (ASTNode) (astt.getChild(0));
    int i = withAst.getChildCount();
    if (i < 2) {
      throw new SemanticException(ErrorMsg.WITH_CHECK_ERROR.getMsg(withAst));
    }
    for (int j = 0; j < i - 1; j++) {
      ASTNode ch = ((ASTNode) withAst.getChild(j));
      int m = ch.getChildCount();
      if (m != 2) {
        throw new SemanticException(ErrorMsg.WITH_CHECK_ERROR.getMsg(withAst));
      }
      dealWithSubQ(tree, ch);
      tree.put(((ASTNode) ch.getChild(1)).getText().toLowerCase(),
          ((ASTNode) ch));
    }
    ASTNode lastquery = ((ASTNode) withAst.getChild(i - 1));
    if (((ASTNode) lastquery.getChild(0)).getToken().getType() == HiveParser.TOK_FROM) {
      ASTNode frm = ((ASTNode) lastquery.getChild(0));
      chwithtree(tree, frm);
    }
    if (astt.getChildCount() == 1) {
      astt.setChild(0, ((ASTNode) lastquery.getChild(0)));
      for (int iiii = 1; iiii < lastquery.getChildCount(); iiii++) {
        astt.addChild(((ASTNode) lastquery.getChild(iiii)));
      }
    } else if (astt.getChildCount() == 2) {
      astt.setChild(0, ((ASTNode) lastquery.getChild(0)));
      ASTNode chinsert = ((ASTNode) astt.deleteChild(1));
      for (int iiii = 1; iiii < lastquery.getChildCount(); iiii++) {
        ASTNode tmpastnode = ((ASTNode) lastquery.getChild(iiii));
        if (tmpastnode.token.getType() != HiveParser.TOK_INSERT) {
          astt.addChild(tmpastnode);
        } else {
          ASTNode tmpselect = ((ASTNode) tmpastnode.getChild(1));
          if (tmpselect != null
              && (tmpselect.getToken().getType() == HiveParser.TOK_SELECT || tmpselect
                  .getToken().getType() == HiveParser.TOK_SELECTDI)) {
            if (chinsert.getChildCount() >= 2) {
              chinsert.setChild(1, tmpselect);
              for (int xjj = 2; xjj < tmpastnode.getChildCount(); xjj++) {
                chinsert.addChild((ASTNode) tmpastnode.getChild(xjj));
              }
            }
          }
        }
      }
      astt.addChild(chinsert);
    }
    return astt;
  }

  ASTNode testWithAndInsert(ASTNode input) {
    if (input.getChildCount() == 2) {
      ASTNode ch1 = (ASTNode) input.getChild(0);
      ASTNode ch2 = (ASTNode) input.getChild(1);
      if ((ch1.getToken().getType() == HiveParser.TOK_WITH)
          && (ch2.getToken().getType() == HiveParser.TOK_INSERT)) {
        return input;
      } else {
        return null;
      }

    } else {
      return null;
    }
  }

  @SuppressWarnings({ "fallthrough", "nls" })
  public void doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();
    boolean skipRecursion = false;
    boolean hasAggregation = false;
    boolean hasAnalysis = false;

    chBetweenInAstAll(ast);
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.OPENWITH)) {
      LinkedHashMap<String, ASTNode> withTree = new LinkedHashMap<String, ASTNode>();
      withTree.clear();
      ASTNode test1 = (ASTNode) (ast.getChild(0));
      if (test1 != null && test1.getToken().getType() == HiveParser.TOK_WITH) {
        dealwithsql(withTree, ast);
      }
      withTree.clear();
      ASTNode test2 = testWithAndInsert(ast);
      if (test2 != null) {
        dealwithsql(withTree, test2);
      }
    }

    if (ast.getToken() != null) {
      skipRecursion = true;
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_SELECTDI:
        qb.countSelDi();
      case HiveParser.TOK_SELECT:
        if (_innerMost_) {
          _innerMost_ = false;
          if (_distinctKeys == null) {
            _distinctKeys = new ArrayList<ToolBox.tableDistinctTuple>();
          }

          if (_groupByKeys == null) {
            _groupByKeys = new ArrayList<ToolBox.tableTuple>();
          }
        }

        doPhase1AdjustNotInNodeAll(ast);

        qb.countSel();
        qbp.setSelExprForClause(ctx_1.dest, ast);

        if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_HINTLIST) {
          qbp.setHints((ASTNode) ast.getChild(0));
        }

        LinkedHashMap<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast);
        doPhase1GetColumnAliasesFromSelect(ast, qbp);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprsForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExpr(aggregations));

        if (!aggregations.isEmpty()) {
          qbp.addHasAggrForClause(ctx_1.dest, true);
        }

        LinkedHashMap<String, ASTNode> groupings = doPhase1GetGroupingFromSelect(ast);
        qbp.setGroupingExprsForClause(ctx_1.dest, groupings);

        LinkedHashMap<String, ASTNode> analysises = doPhase1GetAnalysisesFromSelect(ast);
        if (analysises.entrySet().size() > 0
            && (ast.getToken().getType() == HiveParser.TOK_SELECTDI))
          throw new SemanticException(
              "Not support analysis functions in Select Distinct");

        if (!analysises.isEmpty()) {
          qbp.addHasAnaForClause(ctx_1.dest, true);
        }

        if ((aggregations.entrySet().size() != 0)
            && (analysises.entrySet().size() != 0)) {
          throw new SemanticException(
              "Group by and Analysis Functions should not be in the same SQL!");
        }

        qbp.setAnalysisExprsForClause(ctx_1.dest, analysises);

        List<ASTNode> dists = doPhase1GetDistinctFuncExprFromAnalysisExprs(analysises);
        if (dists != null
            && !qbp.getDistinctFuncExprsForClause(ctx_1.dest).isEmpty()) {
          throw new SemanticException(
              "Distinct for Aggregation and Analysis Functions should not be in the same SQL!");
        }
        qbp.setDistinctFuncOverExprForClause(ctx_1.dest, dists);

        if ((qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI)
            && (analysises.entrySet().size() != 0))
          throw new SemanticException(
              "Select Distinct conflicts with Analysis Functions!");

        doPhase1JudgeAnalysises(analysises, dists);
        break;

      case HiveParser.TOK_VALUES: {
        qbp.setDestToValues(ctx_1.dest, ast);

        String tmpalise = "insertvaluestmptable";
        qb.setInsertTmpTabAlias(tmpalise, new QB.tableRef(SessionState.get()
            .getDbName(), tmpalise, null, null, null));

        ASTNode tmpast = ASTNode.get(HiveParser.TOK_SELECT, "TOK_SELECT");
        ASTNode tmpast1 = ASTNode.get(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
        ASTNode tmpast2 = ASTNode
            .get(HiveParser.TOK_ALLCOLREF, "TOK_ALLCOLREF");
        tmpast.addChild(tmpast1);
        tmpast1.addChild(tmpast2);
        qbp.setSelExprForClause(ctx_1.dest, tmpast);
        qbp.setAggregationExprsForClause(ctx_1.dest,
            new LinkedHashMap<String, ASTNode>());
        qbp.setDistinctFuncExprsForClause(ctx_1.dest, null);
        qbp.setAnalysisExprsForClause(ctx_1.dest,
            new LinkedHashMap<String, ASTNode>());
        qbp.setDistinctFuncOverExprForClause(ctx_1.dest, null);
      }
        break;

      case HiveParser.TOK_WHERE: {
        doPhase1AdjustNotInNodeAll(ast);
        qbp.setWhrExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_APPENDDESTINATION:
      case HiveParser.TOK_DESTINATION: {
        ctx_1.dest = "insclause-" + ctx_1.nextNum;
        ctx_1.nextNum++;

        boolean overwrite = ast.getToken().getType() == HiveParser.TOK_DESTINATION;

        if (qbp.getIsSubQ()) {
          ASTNode ch = (ASTNode) ast.getChild(0);
          if ((ch.getToken().getType() != HiveParser.TOK_DIR)
              || (((ASTNode) ch.getChild(0)).getToken().getType() != HiveParser.TOK_TMP_FILE))
            throw new SemanticException(
                ErrorMsg.NO_INSERT_INSUBQUERY.getMsg(ast));
        } else {
          ASTNode ch = (ASTNode) ast.getChild(0);
          if ((ch.getToken().getType() == HiveParser.TOK_DIR)
              && (((ASTNode) ch.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE)) {
            qbp.setIsSelectQuery(true);
          }
        }

        LOG.debug("Insert to dest " + ctx_1.dest + " : overwrite = " + overwrite);
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Insert to dest " + ctx_1.dest + " : overwrite = " + overwrite);
        qbp.setDestForClause(ctx_1.dest, (ASTNode) ast.getChild(0), overwrite);
      }
        break;

      case HiveParser.TOK_FROM: {
        int child_count = ast.getChildCount();
        if (child_count != 1)
          throw new SemanticException("Multiple Children " + child_count);

        doPhase1AdjustNotInNodeAll(ast);

        qbp.setHasFromClause(true);

        ASTNode frm = (ASTNode) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF) {
          String tab = frm.getChild(0).getChild(0).getText().toLowerCase();
          if (!this.hasWith || frm.getChild(0).getChildCount() > 1
              || !this.withQueries.contains(tab))
            processTable(qb, frm);
          else {
            int idx = this.withQueries.indexOf(tab);
            if (this.allTbls != null && this.allTbls.contains(tab))
              throw new SemanticException(
                  "a subquery in with conflicts with some table/subquery/lateralview name: "
                      + tab);
            this.withCounts.put(tab, this.withCounts.get(tab) + 1);
            processSubQuery(qb, this.withMap.get(tab), idx);
          }

        } else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY) {
          processSubQuery(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
          processLateralView(qb, frm);
        } else if (isJoinToken(frm)) {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }
      }
        break;

      case HiveParser.TOK_CLUSTERBY: {

        LOG.debug("The where ast: " + ast.toStringTree());
        doPhase1AdjustNotInNodeAll(ast);

        qbp.setClusterByExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_DISTRIBUTEBY: {
        LOG.debug("The where ast: " + ast.toStringTree());
        doPhase1AdjustNotInNodeAll(ast);

        qbp.setDistributeByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(
              ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg(ast));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(
              ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg(ast));
        }
      }
        break;

      case HiveParser.TOK_SORTBY: {

        canUseIndex = false;

        LOG.debug("The where ast: " + ast.toStringTree());
        doPhase1AdjustNotInNodeAll(ast);

        qbp.setSortByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(
              ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg(ast));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(
              ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg(ast));
        }

      }
        break;

      case HiveParser.TOK_ORDERBY: {
        canUseIndex = false;

        LOG.debug("The where ast: " + ast.toStringTree());
        doPhase1AdjustNotInNodeAll(ast);

        qbp.setOrderByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(
              ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg(ast));
        }
      }
        break;

      case HiveParser.TOK_GROUPBY: {

        doPhase1AdjustNotInNodeAll(ast);

        qbp.addHasAggrForClause(ctx_1.dest, true);

        canUseIndex = false;
        if (qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
          throw new SemanticException(
              ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg(ast));
        }
        _analyzeGroupByASTNode(ast);
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;
      }
        break;

      case HiveParser.TOK_HAVING:
        LOG.debug("The having ast: " + ast.toStringTree());
        doPhase1AdjustNotInNodeAll(ast);
        LOG.debug("The having ast: " + ast.toStringTree());

        qbp.setHavingExprForClause(ctx_1.dest, ast);
        qbp.addAggregationExprsForClause(ctx_1.dest,
            doPhase1GetAggregationsFromSelect(ast));

        break;

      case HiveParser.TOK_LIMIT: {
        qbp.setDestLimit(ctx_1.dest, new Integer(ast.getChild(0).getText()));
      }
        break;

      case HiveParser.TOK_UNION:
        canUseIndex = false;
        if (!qbp.getIsSubQ())
          throw new SemanticException(ErrorMsg.UNION_NOTIN_SUBQ.getMsg());

      default:
        skipRecursion = false;
        break;
      }
    }

    if (qbp.getHasAggrForClause(ctx_1.dest) != null
        && qbp.getHasAnaForClause(ctx_1.dest) != null) {
      if (qbp.getHasAggrForClause(ctx_1.dest)
          && qbp.getHasAnaForClause(ctx_1.dest))
        throw new SemanticException(
            "Can not use Aggregation Functions and Analysis Functions in the same SQL");
    }

    if (!skipRecursion) {
      int child_count = ast.getChildCount();
      for (int child_pos = 0; child_pos < child_count; ++child_pos) {

        doPhase1((ASTNode) ast.getChild(child_pos), qb, ctx_1);
      }
    }
  }

  private LinkedHashMap<String, ASTNode> doPhase1GetGroupingFromSelect(
      ASTNode selExpr) {
    LinkedHashMap<String, ASTNode> groupingTrees = new LinkedHashMap<String, ASTNode>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode sel = (ASTNode) selExpr.getChild(i).getChild(0);
      doPhase1GetAllGroupings(sel, groupingTrees);
    }
    return groupingTrees;
  }

  private void doPhase1GetAllGroupings(ASTNode expressionTree,
      LinkedHashMap<String, ASTNode> groupingTrees) {

    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        if (functionName.equalsIgnoreCase("grouping")) {
          groupingTrees.put(expressionTree.toStringTree(), expressionTree);
          return;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllGroupings((ASTNode) expressionTree.getChild(i),
          groupingTrees);
    }

  }

  private void doPhase1AdjustNotInNodeAll(ASTNode currentTree)
      throws SemanticException {

    for (int i = 0; i < currentTree.getChildCount(); i++) {
      doPhase1AdjustNotInNode(currentTree, i, (ASTNode) currentTree.getChild(i));
    }
  }

  private void doPhase1AdjustNotInNode(ASTNode fatherTree, int idx,
      ASTNode currentTree) throws SemanticException {

    if (currentTree.getToken().getType() == HiveParser.KW_IN
        || currentTree.getToken().getType() == HiveParser.KW_LIKE
        || currentTree.getToken().getType() == HiveParser.KW_RLIKE) {
      LOG.debug("current tree: " + currentTree.toStringTree());
      ASTNode child0 = (ASTNode) currentTree.getChild(0);
      if (child0.getToken().getType() == HiveParser.KW_NOT) {
        ASTNode child00 = (ASTNode) child0.getChild(0);
        child0.setChild(0, currentTree);
        fatherTree.setChild(idx, child0);
        currentTree.setChild(0, child00);
      }
      LOG.debug("new tree: " + currentTree.toStringTree());

    }

    for (int i = 0; i < currentTree.getChildCount(); i++) {
      doPhase1AdjustNotInNode(currentTree, i, (ASTNode) currentTree.getChild(i));
    }
  }

  private void genPartitionPruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genPartitionPruners(qbexpr.getQB());
    } else {
      genPartitionPruners(qbexpr.getQBExpr1());
      genPartitionPruners(qbexpr.getQBExpr2());
    }
  }

  @SuppressWarnings("nls")
  private void genPartitionPruners(QB qb) throws SemanticException {
    Map<String, Boolean> joinPartnPruner = new HashMap<String, Boolean>();
    QBParseInfo qbp = qb.getParseInfo();

    for (String alias : qb.getSubqAliases()) {
      if (this.hasWith && this.withQueries.contains(alias)) {
        int idx = this.withQueries.indexOf(alias);
        if (this.beProcessed.get(idx))
          continue;
        if (this.withCounts.get(alias) > 1) {
          this.beProcessed.set(idx, true);
          continue;
        } else
          this.beProcessed.set(idx, true);
      }
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genPartitionPruners(qbexpr);
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);

      org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = new org.apache.hadoop.hive.ql.parse.ASTPartitionPruner(
          alias, qb, conf);

      for (String clause : qbp.getClauseNames()) {

        ASTNode whexp = (ASTNode) qbp.getWhrForClause(clause);
        if (whexp != null) {
          pruner.addExpression((ASTNode) whexp.getChild(0), unparseTranslator);
        }
      }

      this.aliasToPruner.put(alias_id, pruner);
    }

    if (!qb.getTabAliases().isEmpty() && qb.getQbJoinTree() != null) {
      int pos = 0;
      for (String alias : qb.getQbJoinTree().getBaseSrc()) {
        if (alias != null) {
          String alias_id = (qb.getId() == null ? alias : qb.getId() + ":"
              + alias);
          org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = this.aliasToPruner
              .get(alias_id);
          if (pruner == null) {
            pos++;
            continue;
          }
          Vector<ASTNode> filters = qb.getQbJoinTree().getFilters().get(pos);
          for (ASTNode cond : filters) {
            pruner.addJoinOnExpression(cond, unparseTranslator);
            if (pruner.hasPartitionPredicate(cond))
              joinPartnPruner.put(alias_id, new Boolean(true));
          }
          if (qb.getQbJoinTree().getJoinSrc() != null) {
            filters = qb.getQbJoinTree().getFilters().get(0);
            for (ASTNode cond : filters) {
              pruner.addJoinOnExpression(cond, unparseTranslator);
              if (pruner.hasPartitionPredicate(cond))
                joinPartnPruner.put(alias_id, new Boolean(true));
            }
          }
        }
        pos++;
      }
    }

    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEOPTPPD)
        || !HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEOPTPPR)) {
      for (String alias : qb.getTabAliases()) {
        String alias_id = (qb.getId() == null ? alias : qb.getId() + ":"
            + alias);
        org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = this.aliasToPruner
            .get(alias_id);
        if (joinPartnPruner.get(alias_id) == null) {
          for (String clause : qbp.getClauseNames()) {

            ASTNode whexp = (ASTNode) qbp.getWhrForClause(clause);
            if (pruner.getTable().isPartitioned()
                && conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE)
                    .equalsIgnoreCase("strict")
                && (whexp == null || !pruner
                    .hasPartitionPredicate((ASTNode) whexp.getChild(0)))) {
              throw new SemanticException(
                  ErrorMsg.NO_PARTITION_PREDICATE.getMsg(whexp != null ? whexp
                      : qbp.getSelForClause(clause), " for Alias " + alias
                      + " Table " + pruner.getTable().getName()));
            }
          }
        }
      }
    }
  }

  private void genSamplePruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genSamplePruners(qbexpr.getQB());
    } else {
      genSamplePruners(qbexpr.getQBExpr1());
      genSamplePruners(qbexpr.getQBExpr2());
    }
  }

  @SuppressWarnings("nls")
  private void genSamplePruners(QB qb) throws SemanticException {
    for (String alias : qb.getSubqAliases()) {
      if (this.hasWith && this.withQueries.contains(alias))
        continue;
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genSamplePruners(qbexpr);
    }
    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      QBParseInfo qbp = qb.getParseInfo();
      TableSample tableSample = qbp.getTabSample(alias_id);
      if (tableSample != null) {
        SamplePruner pruner = new SamplePruner(alias, tableSample);
        this.aliasToSamplePruner.put(alias_id, pruner);
      }
    }
  }

  private void getMetaData(QBExpr qbexpr) throws SemanticException,
      MetaException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getMetaData(qbexpr.getQB());
    } else {
      getMetaData(qbexpr.getQBExpr1());
      getMetaData(qbexpr.getQBExpr2());
    }
  }

  private Table getTblFromCache(String dbName, String tblName,
      Map<String, Table> tbls, int optimizeLevel) throws HiveException {
    assert (dbName != null);
    assert (tblName != null);
    Table tbl = null;

    switch (optimizeLevel) {
    case 0:
      tbl = this.db.getTable(dbName, tblName);
      break;

    case 1:
    case 2:
    default:
      if (tbls != null && !tbls.isEmpty()) {
        tbl = tbls.get(dbName + "/" + tblName);
      }

      if (tbl == null) {
        tbl = this.db.getTable(dbName, tblName);
        if (tbls != null && !tbls.isEmpty()) {
          tbls.put(dbName + "/" + tblName, tbl);
        }
      }

      break;
    }

    return tbl;
  }

  @SuppressWarnings("nls")
  public void getMetaData(QB qb) throws SemanticException {
    try {
      LOG.debug("Get metadata for source tables");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Get metadata for source tables");

      String qbAlias = null;
      int idx = -1;
      if (qb.getParseInfo().getAlias() != null) {
        qbAlias = qb.getParseInfo().getAlias().toLowerCase();

        if (this.hasWith && qbAlias != null && withQueries.contains(qbAlias)) {
          idx = this.withQueries.indexOf(qbAlias);
          if (this.beProcessed.get(idx))
            return;
        }
      }

      int aliasNum = qb.getTabAliases().size();
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
      Map<String, String> aliasToViewName = new HashMap<String, String>();

      int optimizerLevel = conf.getInt(
          "hive.semantic.analyzer.optimizer.level", 2);

      for (String alias : tabAliases) {
        String tab_name = qb.getTabNameForAlias(alias);
        String db_name = qb.getTableRef(alias).getDbName();
        Table tab = null;

        Boolean isFirst = null;
        switch (optimizerLevel) {
        case 0:

          try {
            try {
              this.db.getDatabase(db_name);
            } catch (Exception e) {
              throw new SemanticException("get  database : " + db_name
                  + " error,make sure it exists!");
            }

            if (!db.hasAuth(SessionState.get().getUserName(),
                Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
              if(db.isLhotseImportExport() && 
                  db.isHdfsExternalTable(db_name, tab_name)){
                if (SessionState.get() != null)
                  SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                      + " do not have select privilege on table : "
                      + db_name + "::" + tab_name
                      + ",but it's a lhotse import or export task");
              }else{
                throw new SemanticException("user : "
                    + SessionState.get().getUserName()
                    + " do not have SELECT privilege on table : " + db_name
                    + "::" + tab_name);
              }
            }

            if (qb.isCTAS()) {
              if (!db.hasAuth(SessionState.get().getUserName(),
                  Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),
                  null)) {
                if (SessionState.get() != null)
                  SessionState.get().ssLog(
                      "user : " + SessionState.get().getUserName()
                          + " do not have CREATE privilege on DB : "
                          + SessionState.get().getDbName());
                throw new SemanticException("user : "
                    + SessionState.get().getUserName()
                    + " do not have CREATE privilege on DB : "
                    + SessionState.get().getDbName());
              }
            }

            tab = this.db.getTable(db_name, tab_name);
          } catch (InvalidTableException ite) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
                .getParseInfo().getSrcForAlias(alias)));
          }
          break;

        case 1:
          isFirst = firstItemMap.get(db_name + "/" + tab_name);

          if (allTableMap != null && allTableMap.size() > 0 && isFirst != null
              && isFirst.booleanValue()) {
            tab = allTableMap.get(db_name + "/" + tab_name);
            firstItemMap.put(db_name + "/" + tab_name, false);
          }

          if (tab == null) {
            try {
              try {
                this.db.getDatabase(db_name);
              } catch (Exception e) {
                throw new SemanticException("get   database : " + db_name
                    + " error,make sure it exists!");
              }

              if (!db.hasAuth(SessionState.get().getUserName(),
                  Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
                if(db.isLhotseImportExport() && 
                    db.isHdfsExternalTable(db_name, tab_name)){
                  if (SessionState.get() != null)
                    SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                        + " do not have select privilege on table : "
                        + db_name + "::" + tab_name
                        + ",but it's a lhotse import or export task");
                }else{
                  throw new SemanticException("user : "
                      + SessionState.get().getUserName()
                      + " do not have SELECT privilege on table : " + db_name
                      + "::" + tab_name);
                }
              }

              if (qb.isCTAS()) {
                if (!db.hasAuth(SessionState.get().getUserName(),
                    Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),
                    null)) {
                  if (SessionState.get() != null)
                    SessionState.get().ssLog(
                        "user : " + SessionState.get().getUserName()
                            + " do not have CREATE privilege on DB : "
                            + SessionState.get().getDbName());
                  throw new SemanticException("user : "
                      + SessionState.get().getUserName()
                      + " do not have CREATE privilege on DB : "
                      + SessionState.get().getDbName());
                }
              }

              tab = this.db.getTable(db_name, tab_name);
              allTableMap.put(db_name + "/" + tab_name, tab);
              firstItemMap.put(db_name + "/" + tab_name, false);
            } catch (InvalidTableException ite) {
              throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
                  .getParseInfo().getSrcForAlias(alias)));
            }
          } else {
            if (!db.hasAuth(SessionState.get().getUserName(),
                Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
              if(db.isLhotseImportExport() && 
                  db.isHdfsExternalTable(db_name, tab_name)){
                if (SessionState.get() != null)
                  SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                      + " do not have select privilege on table : "
                      + db_name + "::" + tab_name
                      + ",but it's a lhotse import or export task");
              }else{
                throw new SemanticException("user : "
                    + SessionState.get().getUserName()
                    + " do not have SELECT privilege on table : " + db_name
                    + "::" + tab_name);
              }
            }
          }
          break;

        case 2:
        default:

          Table tabOld = null;
          if (allTableMap != null && allTableMap.size() > 0) {
            tabOld = allTableMap.get(db_name + "/" + tab_name);
          }

          if (tabOld == null) {
            try {
              try {
                this.db.getDatabase(db_name);
              } catch (Exception e) {
                throw new SemanticException("get   database : " + db_name
                    + " error,make sure it exists!");
              }

              if (!db.hasAuth(SessionState.get().getUserName(),
                  Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
                if(db.isLhotseImportExport() && 
                    db.isHdfsExternalTable(db_name, tab_name)){
                  if (SessionState.get() != null)
                    SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                        + " do not have select privilege on table : "
                        + db_name + "::" + tab_name
                        + ",but it's a lhotse import or export task");
                }else{
                  throw new SemanticException("user : "
                      + SessionState.get().getUserName()
                      + " do not have SELECT privilege on table : " + db_name
                      + "::" + tab_name);
                }
              }

              if (qb.isCTAS()) {
                if (!db.hasAuth(SessionState.get().getUserName(),
                    Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),
                    null)) {
                  if (SessionState.get() != null)
                    SessionState.get().ssLog(
                        "user : " + SessionState.get().getUserName()
                            + " do not have CREATE privilege on DB : "
                            + SessionState.get().getDbName());
                  throw new SemanticException("user : "
                      + SessionState.get().getUserName()
                      + " do not have CREATE privilege on DB : "
                      + SessionState.get().getDbName());
                }
              }

              tab = this.db.getTable(db_name, tab_name);
              allTableMap.put(db_name + "/" + tab_name, tab);
              firstItemMap.put(db_name + "/" + tab_name, false);
            } catch (InvalidTableException ite) {
              throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
                  .getParseInfo().getSrcForAlias(alias)));
            }
          } else {
            if (!db.hasAuth(SessionState.get().getUserName(),
                Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
              if(db.isLhotseImportExport() && 
                  db.isHdfsExternalTable(db_name, tab_name)){
                if (SessionState.get() != null)
                  SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                      + " do not have select privilege on table : "
                      + db_name + "::" + tab_name
                      + ",but it's a lhotse import or export task");
              }else{
                throw new SemanticException("user : "
                    + SessionState.get().getUserName()
                    + " do not have SELECT privilege on table : " + db_name
                    + "::" + tab_name);
              }
            }

            if (qb.isCTAS()) {
              if (!db.hasAuth(SessionState.get().getUserName(),
                  Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),
                  null)) {
                if (SessionState.get() != null)
                  SessionState.get().ssLog(
                      "user : " + SessionState.get().getUserName()
                          + " do not have CREATE privilege on DB : "
                          + SessionState.get().getDbName());
                throw new SemanticException("user : "
                    + SessionState.get().getUserName()
                    + " do not have CREATE privilege on DB : "
                    + SessionState.get().getDbName());
              }
            }

            isFirst = firstItemMap.get(db_name + "/" + tab_name);

            if (isFirst != null && isFirst.booleanValue()) {
              tab = tabOld;
              firstItemMap.put(db_name + "/" + tab_name, false);
            } else {
              tab = this.db.cloneTable(tabOld.getTTable());
            }
          }
          break;
        }

        try {

          String priPart = qb.getTableRef(alias).getPriPart();
          String subPart = qb.getTableRef(alias).getSubPart();
          if (priPart != null) {
            if (tab.isView())
              throw new SemanticException("can not use partition with view!");

            if (!tab.isPartitioned())
              throw new SemanticException(
                  "can not use partition with a non-partition table!");

            if (!tab.getTTable().getPriPartition().getParSpaces()
                .containsKey(priPart))
              throw new SemanticException("wrong pri-partition name: "
                  + priPart);

            if (subPart != null) {
              if (!tab.getHasSubPartition())
                throw new SemanticException(
                    "can not use sub partition with a non-subpartition table!");

              if (!tab.getTTable().getSubPartition().getParSpaces()
                  .containsKey(subPart))
                throw new SemanticException("wrong sub-parititon name: "
                    + subPart);
            }
          }

          LOG.debug("tab_name:" + tab_name + ",db_name:" + db_name);
          if (createVwDesc != null && !tab.isView()) {
            String tName = db_name + "::" + tab_name;
            createVwDesc.addVTable(tName);
          }
          String jarPath = tab.getProperty(Constants.PB_JAR_PATH);
          if (jarPath != null && !jarPath.isEmpty()) {
            SessionState.get().add_resource(SessionState.ResourceType.JAR,
                jarPath);
          }
          String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
          LOG.debug("++++++++++++++" + tableServer);
          if (tableServer != null && !tableServer.isEmpty()
              && !isAllDBExternalTable && !ctx.getExplain()) {
            if (dbDataTmpFilePathes.containsKey(tab.getDbName() + "/"
                + tab.getName())) {
              tab.setDataLocation(new URI(dbDataTmpFilePathes.get(tab
                  .getDbName() + "/" + tab.getName())));
              LOG.debug("++++++++++++++already exist,no need to load!");
            } else {
              ExtractConfig config = new ExtractConfig();
              config.setHost(tableServer);
              String port = tab.getSerdeParam(Constants.TABLE_PORT);
              config.setPort(port);
              String db = tab.getSerdeParam(Constants.TBALE_DB);
              String user = tab.getSerdeParam(Constants.DB_URSER);
              String pwd = tab.getSerdeParam(Constants.DB_PWD);
              String type = tab.getSerdeParam(Constants.DB_TYPE);
              config.setDbName(db);
              config.setUser(user);
              config.setPwd(pwd);
              config.setConf(conf);
              config.setDbType(type);
              String table = tab.getSerdeParam(Constants.TABLE_NAME);
              String sql = "";
              if (table != null && !table.isEmpty()) {
                sql = "select * from " + table;
              } else {
                sql = tab.getSerdeParam(Constants.TBALE_SQL);
              }
              String tmpDir = conf.getVar(HiveConf.ConfVars.SCRATCHDIR);
              String filePath = tmpDir + "/" + conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "_" + Utilities.randGen.nextInt();
              config.setSql(sql);
              config.setFilePath(filePath);
              IFetchSize fetch = new MemoryAdaptiveFetchSize(config);
              config.setBufferLimit(fetch.computeFetchSize(config));
              BaseDataExtract dataExtract = DataExtractFactory
                  .getDataExtractTool(type);
              String hdfsDefalutName = null;
              if (multiHdfsInfo.isMultiHdfsEnable()) {
                hdfsDefalutName = multiHdfsInfo.getHdfsPathFromDB(tab
                    .getDbName());
                LOG.info("extract the PG table to the hdfs:" + hdfsDefalutName);
                tab.setDataLocation(new Path(hdfsDefalutName, filePath).toUri());
                config.setHdfsfilePath(new Path(hdfsDefalutName, filePath));
                dbDataTmpFilePathes.put(tab.getDbName() + "/" + tab.getName(),
                    new Path(hdfsDefalutName, filePath).toString());
              }
              dataExtract.setConfig(config);
              dataExtract.extractDataToHive();
              Properties p = conf.getAllProperties();
              if (!multiHdfsInfo.isMultiHdfsEnable()) {
                hdfsDefalutName = p.getProperty("fs.defaultFS");
                if(hdfsDefalutName == null){
                hdfsDefalutName = p.getProperty("fs.default.name");
                }
                tab.setDataLocation(new URI(hdfsDefalutName + filePath));
                dbDataTmpFilePathes.put(tab.getDbName() + "/" + tab.getName(),
                    hdfsDefalutName + filePath);
              }
            }

          }
          if (aliasNum < 2) {
            indexQueryInfo.paramMap = tab.getParameters();
            indexQueryInfo.dbName = SessionState.get().getDbName();
            indexQueryInfo.tblName = tab_name;
            allColsList = tab.getAllCols();
            indexQueryInfo.fieldNum = allColsList.size();
          }
        } catch (InvalidTableException ite) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
              .getParseInfo().getSrcForAlias(alias)));
        } catch (URISyntaxException e) {
          throw new SemanticException(e.getMessage());
        }

        if (tab.isView()) {
          String fullViewName = tab.getName();
          if (!tab.getName().contains("::"))
            fullViewName = tab.getDbName() + "::" + tab.getName();
          if (viewsExpanded.contains(fullViewName)) {
            throw new SemanticException("Recursive view " + fullViewName
                + " detected (cycle: "
                + StringUtils.join(viewsExpanded, " -> ") + " -> "
                + fullViewName + ").");
          }
          String realUser = SessionState.get().getUserName();
          String viewOwner = "root";
          SessionState.get().setUserName(viewOwner);

          try {
            replaceViewReferenceWithDefinition(qb, tab, tab_name, alias);
          } catch (SemanticException e) {
            SessionState.get().setUserName(realUser);
            throw e;
          }

          SessionState.get().setUserName(realUser);
          aliasToViewName.put(alias, fullViewName);
          continue;
        }

        if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass()))
          throw new SemanticException(
              ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg(qb.getParseInfo()
                  .getSrcForAlias(alias)));

        qb.getMetaData().setSrcForAlias(
            alias,
            new TablePartition(alias, qb.getTableRef(alias).getPriPart(), qb
                .getTableRef(alias).getSubPart(), tab));
      }

      LOG.debug("Get metadata for subqueries");
      if (qb.isCTAS()) {
          if (!db.hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),
              null)) {
            if (SessionState.get() != null)
              SessionState.get().ssLog(
                  "user : " + SessionState.get().getUserName()
                      + " do not have CREATE privilege on DB : "
                      + SessionState.get().getDbName());
            throw new SemanticException("user : "
                + SessionState.get().getUserName()
                + " do not have CREATE privilege on DB : "
                + SessionState.get().getDbName());
          }
      }
      for (String alias : qb.getSubqAliases()) {
        boolean wasView = aliasToViewName.containsKey(alias);
        if (wasView) {
          viewsExpanded.add(aliasToViewName.get(alias));
        }
        QBExpr qbexpr = qb.getSubqForAlias(alias);
        String realUser = null;
        String viewOwner = null;
        if (wasView) {
          String[] tmp = alias.split("/");
          String d_name = null;
          String t_name = null;
          assert (tmp.length == 2);
          d_name = tmp[0];
          t_name = tmp[1];
          String[] tmp2 = tmp[1].split("#");
          t_name = tmp2[0];
          Table viewTab = this.db.getTable(d_name, t_name);
          realUser = SessionState.get().getUserName();
          viewOwner = "root";
          SessionState.get().setUserName(viewOwner);
        }

        try {
          getMetaData(qbexpr);
        } catch (SemanticException e1) {
          if (wasView) {
            SessionState.get().setUserName(realUser);
          }

          throw e1;
        } catch (MetaException e2) {
          if (wasView) {
            SessionState.get().setUserName(realUser);
          }

          throw e2;
        }

        if (wasView) {
          SessionState.get().setUserName(realUser);
        }
        if (wasView) {
          viewsExpanded.remove(viewsExpanded.size() - 1);
        }
      }

      LOG.debug("Get metadata for destination tables");
      QBParseInfo qbp = qb.getParseInfo();

      for (String name : qbp.getClauseNamesForDest()) {
        ASTNode ast = qbp.getDestForClause(name);
        Boolean isoverwrite = qbp.getDestOverwrittenForClause(name);
        switch (ast.getToken().getType()) {

        case HiveParser.TOK_TABDEST: {
          ASTNode ast_tab = (ASTNode) ast.getChild(0);
          tableSpec ts = new tableSpec();
          ts.init(this.db, conf, ast_tab, allTableMap, firstItemMap,
              optimizerLevel);

          if (ts.tableHandle.isView()) {
            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
          }
          if (!db.hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.INSERT_PRIV, ts.dbName, ts.tableName)) {
            if(db.isLhotseImportExport()){
              if (SessionState.get() != null)
                SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                    + " do not have insert privilege on table : "
                    + ts.dbName + "::" + ts.tableName
                    + ",but it's a lhotse import or export task");
            }else{
              throw new SemanticException("user : "
                  + SessionState.get().getUserName()
                  + " do not have insert privilege on table : " + ts.dbName
                  + "::" + ts.tableName);
            }
          }

          Table descTbl = getTblFromCache(ts.dbName, ts.tableName, allTableMap,
              optimizerLevel);

          if (DBExternalTableUtil.isDBExternal(db, SessionState.get()
              .getDbName(), ts.tableName, allTableMap, optimizerLevel)
              && !DBExternalTableUtil
                  .isDBExternalCanInsert(db, SessionState.get().getDbName(),
                      ts.tableName, allTableMap, optimizerLevel)) {
            throw new HiveException(
                "only can insert data into an db external table associathed with an db table.");
          } else if (DBExternalTableUtil.isDBExternalCanInsert(db, SessionState
              .get().getDbName(), ts.tableName, allTableMap, optimizerLevel)) {
            String oText = ctx.getTokenRewriteStream().toString(
                this.ast.getTokenStartIndex(), this.ast.getTokenStopIndex());
            if (oText.toLowerCase().indexOf("overwrite") != -1) {
              throw new HiveException(
                  "sorry!! we cann't support overwrite currently!");
            }
            if (SQLTransfer.isInsertSelectStar(oText)) {
              for (String alias : qb.getTabAliases()) {
                LoadConfig config = new LoadConfig();
                Table tab1 = qb.getMetaData().getTableForAlias(alias).getTbl();
                if (tab1.getTableStorgeType().trim().equalsIgnoreCase("text")) {

                  Table tab = getTblFromCache(SessionState.get().getDbName(),
                      ts.tableName, allTableMap, optimizerLevel);

                  String tableServer = tab
                      .getSerdeParam(Constants.TABLE_SERVER);
                  String port = tab.getSerdeParam(Constants.TABLE_PORT);
                  String db = tab.getSerdeParam(Constants.TBALE_DB);
                  String user = tab.getSerdeParam(Constants.DB_URSER);
                  String pwd = tab.getSerdeParam(Constants.DB_PWD);
                  String type = tab.getSerdeParam(Constants.DB_TYPE);
                  String table = tab.getSerdeParam(Constants.TABLE_NAME);
                  config.setHost(tableServer);
                  config.setPort(port);
                  config.setDbName(db);
                  config.setUser(user);
                  config.setPwd(pwd);
                  config.setDbType(type);
                  config.setTable(tab);
                  config.setDbTable(table);
                  config.setDesc(null);
                  config.setConf(conf);
                  config.setFilePath(tab1.getDataLocation().toString());
                  isDbExternalInsert = true;
                  BaseDBExternalDataLoad load = new BaseDBExternalDataLoad(
                      config);
                  load.loadDataToDBTable();
                  return;
                }
              }
            }
          }
          String jarPath = descTbl.getProperty(Constants.PB_JAR_PATH);
          if (jarPath != null && !jarPath.isEmpty()) {
            SessionState.get().add_resource(SessionState.ResourceType.JAR,
                jarPath);
          }

          if (isoverwrite
              && !db.hasAuth(SessionState.get().getUserName(),
                  Hive.Privilege.DELETE_PRIV, ts.dbName, ts.tableName)) {
            if(db.isLhotseImportExport()){
              if (SessionState.get() != null)
                SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                    + " do not have delete or overwrite privilege on table : "
                    + ts.dbName + "::" + ts.tableName
                    + ",but it's a lhotse import or export task");
            }else{
              throw new SemanticException("user : "
                  + SessionState.get().getUserName()
                  + " do not have delete or overwrite privilege on table : "
                  + ts.dbName + "::" + ts.tableName);
            }
          }

          if (!HiveOutputFormat.class.isAssignableFrom(ts.tableHandle
              .getOutputFormatClass()))
            throw new SemanticException(
                ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg(ast_tab));

          qb.getMetaData().setDestForAlias(name, ts, isoverwrite);

          if (ast.getChildCount() > 1) {
            ArrayList<String> columns = new ArrayList<String>();
            Vector<StructField> fields = ts.tableHandle.getFields();
            for (int i = 1; i < ast.getChildCount(); i++) {
              String colname = ((ASTNode) ast.getChild(i).getChild(0))
                  .getText().toLowerCase();
              boolean include = false;
              for (StructField field : fields) {
                if (field.getFieldName().equalsIgnoreCase(colname)) {
                  include = true;
                  break;
                }
              }
              if (include) {
                columns.add(colname);
              } else {
                throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(ast
                    .getChild(i)));
              }
            }
            qb.getMetaData().addInsertColumnsToDestTable(name, columns);
          }

          ASTNode values = null;
          if ((values = qbp.getDestToValues(name)) != null) {
            if (DBExternalTableUtil.isDBExternalCanInsert(db, SessionState
                .get().getDbName(), ts.tableName, allTableMap, optimizerLevel)
                && !isAllDBExternalTable) {
              LoadConfig config = new LoadConfig();

              Table tab = getTblFromCache(SessionState.get().getDbName(),
                  ts.tableName, allTableMap, optimizerLevel);

              String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
              String port = tab.getSerdeParam(Constants.TABLE_PORT);
              String db = tab.getSerdeParam(Constants.TBALE_DB);
              String user = tab.getSerdeParam(Constants.DB_URSER);
              String pwd = tab.getSerdeParam(Constants.DB_PWD);
              String type = tab.getSerdeParam(Constants.DB_TYPE);
              String table = tab.getSerdeParam(Constants.TABLE_NAME);
              config.setHost(tableServer);
              config.setPort(port);
              config.setDbName(db);
              config.setUser(user);
              config.setPwd(pwd);
              config.setDbType(type);
              config.setTable(tab);
              config.setDbTable(table);
              config.setDesc(null);
              config.setConf(conf);
              config.setFilePath(null);
              String oText = ctx.getTokenRewriteStream().toString(
                  this.ast.getTokenStartIndex(), this.ast.getTokenStopIndex());
              oText = oText.replaceFirst(ts.tableName, table);
              config.setoText(oText);
              isDbExternalInsert = true;
              BaseDBExternalDataLoad load = new BaseDBExternalDataLoad(config);
              load.loadDataToDBTable();
              qbp.setDestToValues(name, null);
              return;
            }
            List<FieldSchema> cols = ts.tableHandle.getAllCols();
            List<? extends StructField> fields = ts.tableHandle.getFields();
            if (qb.getMetaData().getInsertColumnsToDestTable()
                .containsKey(name)) {
              ArrayList<String> columns = qb.getMetaData()
                  .getInsertColumnsToDestTable().get(name);
              List<FieldSchema> cols1 = new ArrayList<FieldSchema>();
              ArrayList<StructField> fields1 = new ArrayList<StructField>();
              for (String col : columns) {
                for (FieldSchema fsch : cols) {
                  if (fsch.getName().equals(col)) {
                    cols1.add(fsch);
                    if (fsch.getType().equalsIgnoreCase("timestamp")) {
                      throw new SemanticException(
                          ErrorMsg.TIMESTAMP_ERROR.getMsg());
                    }
                    break;
                  }
                }
                for (StructField flds : fields) {
                  if (flds.getFieldName().equals(col)) {
                    fields1.add(flds);
                    break;
                  }
                }
              }
              cols = cols1;
              fields = fields1;
            }

            StringBuffer colNames = new StringBuffer();
            StringBuffer colTypes = new StringBuffer();
            for (int k = 0; k < fields.size(); k++) {
              String newColName = "_TMPTABLE_" + k;
              colNames.append(newColName);
              colNames.append(',');
              colTypes.append(fields.get(k).getFieldObjectInspector()
                  .getTypeName());
              colTypes.append(',');
            }
            colNames.setLength(colNames.length() - 1);
            colTypes.setLength(colTypes.length() - 1);

            Properties schema = Utilities.makeProperties(
                org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                    + Utilities.ctrlaCode,
                org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
                colNames.toString(),
                org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
                colTypes.toString());

            Deserializer deserializer = LazySimpleSerDe.class.newInstance();
            deserializer.initialize(conf, schema);
            Class<? extends InputFormat<?, ?>> inputFormatClass = MyTextInputFormat.class;
            Class<?> outputFormatClass = IgnoreKeyTextOutputFormat.class;
            FileSystem fs = null;
            if (!multiHdfsInfo.isMultiHdfsEnable()) {
              fs = FileSystem.get(conf);
            } else {
              fs = new Path(multiHdfsInfo.getHdfsPathFromDB(ts.dbName))
                  .getFileSystem(conf);
            }

            Path tmpdir = new Path("/tmp");
            if (!fs.exists(tmpdir)) {
              fs.mkdirs(tmpdir);
            }
            Random r = new Random();
            Path tmptablefile = new Path(tmpdir, String.valueOf(r
                .nextInt(Integer.MAX_VALUE)));
            while (fs.exists(tmptablefile)) {
              tmptablefile = new Path(tmpdir, String.valueOf(r
                  .nextInt(Integer.MAX_VALUE)));
            }

            Table tmptab = new Table("insertvaluestmptable", schema,
                deserializer, inputFormatClass, outputFormatClass, tmptablefile
                    .makeQualified(fs).toUri(), db);
            tmptab.setFields(cols);

            qb.getMetaData().setSrcForAlias(
                "insertvaluestmptable",
                new TablePartition("insertvaluestmptable", qb
                    .getInsertTmpTableRef("insertvaluestmptable").getPriPart(),
                    qb.getInsertTmpTableRef("insertvaluestmptable")
                        .getSubPart(), tmptab));

            FSDataOutputStream fos = fs.create(tmptablefile);
            boolean haserror = false;
            StringBuffer sb = new StringBuffer();
            char splitch = 1;
            for (int i = 0; i < values.getChildCount(); i++) {
              if (haserror)
                break;
              ASTNode child = (ASTNode) values.getChild(i);
              int num = child.getChildCount();
              if (num != cols.size()) {
                haserror = true;
                break;
              }
              for (int j = 0; j < num; j++) {
                ASTNode childj = (ASTNode) child.getChild(j);
                if (childj.getChildCount() > 0) {
                  haserror = true;
                  break;
                }
                exprNodeDesc exp = genExprNodeDesc(childj, new RowResolver(),
                    qb);
                String val = "\\N";
                if (exp instanceof exprNodeConstantDesc) {
                  val = String.valueOf(((exprNodeConstantDesc) exp).getValue());
                }
                sb.append(val).append(splitch);
              }
              sb.delete(sb.length() - 1, sb.length());
              sb.append("\r\n");
            }
            qbp.setDestToValues(name, null);
            if (haserror) {
              fos.close();
              throw new SemanticException(
                  "there are some errors in values clause");
            }
            fos.write(sb.toString().getBytes("utf-8"));
            fos.close();
          }

          break;
        }
        case HiveParser.TOK_DIR: {
          String fname = stripQuotes(ast.getChild(0).getText());
          if ((!qb.getParseInfo().getIsSubQ())
              && (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE)) {
            fname = ctx.getMRTmpFileURI();
            ctx.setResDir(new Path(fname));
            if (qb.isCTAS()) {
              qb.setIsQuery(false);
            } else {
              qb.setIsQuery(true);
            }
          }
          qb.getMetaData().setDestForAlias(name, fname,
              (ast.getToken().getType() == HiveParser.TOK_DIR), isoverwrite);
          qb.getMetaData()
              .setDestForTmpdir(
                  name,
                  ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE);
          break;
        }
        default:
          throw new SemanticException("Unknown Token Type "
              + ast.getToken().getType());
        }
      }

      if (this.hasWith && qbAlias != null
          && withQueries.contains(qbAlias.toLowerCase()))
        this.beProcessed.set(idx, true);
    } catch (HiveException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    } catch (MetaException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    } catch (AccessControlException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (SerDeException e) {
      e.printStackTrace();
    }
  }

  private void replaceViewReferenceWithDefinition(QB qb, Table tab,
      String tab_name, String alias) throws SemanticException {

    ParseDriver pd = new ParseDriver();
    ASTNode viewTree;
    final ASTNodeOrigin viewOrigin = new ASTNodeOrigin("VIEW", tab.getName(),
        tab.getViewExpandedText(), alias, qb.getParseInfo().getSrcForAlias(
            alias));
    try {
      String viewText = tab.getViewExpandedText();
      ASTNode tree = pd.parse(viewText, null);
      tree = ParseUtils.findRootNonNullToken(tree);
      viewTree = tree;
      Dispatcher nodeOriginDispatcher = new Dispatcher() {
        public Object dispatch(Node nd, java.util.Stack<Node> stack,
            Object... nodeOutputs) {
          ((ASTNode) nd).setOrigin(viewOrigin);
          return null;
        }
      };
      GraphWalker nodeOriginTagger = new DefaultGraphWalker(
          nodeOriginDispatcher);
      nodeOriginTagger.startWalking(
          java.util.Collections.<Node> singleton(viewTree), null);
    } catch (ParseException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      StringBuilder sb = new StringBuilder();
      sb.append(e.getMessage());
      ErrorMsg.renderOrigin(sb, viewOrigin);
      throw new SemanticException(sb.toString(), e);
    }
    QBExpr qbexpr = new QBExpr(alias);
    doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias);
    qb.rewriteViewToSubq(alias, tab_name, qbexpr);
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list)
      if (s.equals(elem))
        return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void parseJoinCondPopulateAlias(QBJoinTree joinTree, ASTNode condn,
      Vector<String> leftAliases, Vector<String> rightAliases,
      ArrayList<String> fields, QB qb) throws SemanticException {
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(condn.getChild(0).getText()
          .toLowerCase());
      unparseTranslator.addIdentifierTranslation((ASTNode) condn.getChild(0));
      String db = null;
      String fullAlias = null;

      if (condn.getChildCount() == 2) {
        db = unescapeIdentifier(condn.getChild(1).getText().toLowerCase());
        fullAlias = db + "/" + tableOrCol;
        if (qb.getUserAliasFromDBTB(fullAlias) != null) {
          if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
            throw new SemanticException("table : " + fullAlias
                + "  has more than one alias : "
                + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
          }
          fullAlias = fullAlias + "#"
              + qb.getUserAliasFromDBTB(fullAlias).get(0);
        }

        LOG.debug("fullAlias is full db/tb name : " + fullAlias);

      } else {
        db = SessionState.get().getDbName();

        if (qb.existsUserAlias(tableOrCol)) {
          if (!qb.getSubqAliases().contains(tableOrCol.toLowerCase())) {
            if (qb.exisitsDBTB(db + "/" + tableOrCol)
                && !(qb.getTableRefFromUserAlias(tableOrCol).getDbName() + "/" + qb
                    .getTableRefFromUserAlias(tableOrCol).getTblName())
                    .equalsIgnoreCase(db + "/" + tableOrCol))
              throw new SemanticException("name : " + tableOrCol
                  + " ambigenous,it may be : " + db + "/" + tableOrCol + " or "
                  + qb.getTableRefFromUserAlias(tableOrCol).getDbName() + "/"
                  + qb.getTableRefFromUserAlias(tableOrCol).getTblName());

            LOG.debug("fullAlias is user alias : " + fullAlias);
            fullAlias = qb.getTableRefFromUserAlias(tableOrCol).getDbName()
                + "/" + qb.getTableRefFromUserAlias(tableOrCol).getTblName()
                + "#" + tableOrCol;
          } else {
            LOG.debug("fullAlias is sub query alias : " + fullAlias);
            fullAlias = tableOrCol;
          }
        } else if (qb.exisitsDBTB(db + "/" + tableOrCol)) {
          fullAlias = db + "/" + tableOrCol;
          if (qb.getUserAliasFromDBTB(fullAlias) != null) {
            if (qb.getUserAliasFromDBTB(fullAlias).size() > 1)
              throw new SemanticException("table : " + fullAlias
                  + "  has more than one alias : "
                  + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                  + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
            fullAlias = fullAlias + "#"
                + qb.getUserAliasFromDBTB(fullAlias).get(0);
          }

          LOG.debug("fullAlias is default db/table alias : " + fullAlias);
        } else {
          LOG.debug("fullAlias is null");
        }
      }

      if (fullAlias != null) {
        LOG.debug("--------------------joinTree.getRightAliases(): "
            + joinTree.getRightAliases());
        LOG.debug("fullAlias.toLowerCase() " + fullAlias.toLowerCase());
        if (isPresent(joinTree.getLeftAliases(), fullAlias.toLowerCase())) {
          if (!leftAliases.contains(fullAlias.toLowerCase()))
            leftAliases.add(fullAlias.toLowerCase());
        } else if (isPresent(joinTree.getRightAliases(),
            fullAlias.toLowerCase())) {
          if (!rightAliases.contains(fullAlias.toLowerCase()))
            rightAliases.add(fullAlias.toLowerCase());
        } else {
          throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(condn
              .getChild(0)));
        }
      } else {
        String left_table = null;
        String right_table = null;

        for (String alias : joinTree.getLeftAliases()) {
          TablePartition tab = qb.getMetaData().getSrcForAlias(alias);
          if (tab != null)
            LOG.info("table name: " + tab.getName());

          if (tab == null) {
            throw new SemanticException(
                ErrorMsg.INVALID_JOIN_CONDITION_6.getMsg() + tableOrCol);
          }
          List<FieldSchema> all_cols;
          all_cols = tab.getAllCols();
          for (int i = 0; i < all_cols.size(); i++) {
            String fieldName = all_cols.get(i).getName();
            if (fieldName.equalsIgnoreCase(tableOrCol)) {
              if (left_table == null)
                left_table = alias;
              else
                throw new SemanticException(
                    ErrorMsg.INVALID_JOIN_CONDITION_4.getMsg() + tableOrCol
                        + " in on clause is ambiguous ");
              break;
            }
          }

        }

        for (String alias : joinTree.getRightAliases()) {
          TablePartition tab = qb.getMetaData().getSrcForAlias(alias);
          if (tab == null) {
            throw new SemanticException("Error join table name");
          }

          List<FieldSchema> all_cols;
          all_cols = tab.getAllCols();
          for (int i = 0; i < all_cols.size(); i++) {
            String fieldName = all_cols.get(i).getName();
            if (fieldName.equalsIgnoreCase(tableOrCol)) {
              if (right_table == null)
                right_table = alias;
              else
                throw new SemanticException(
                    ErrorMsg.INVALID_JOIN_CONDITION_4.getMsg() + tableOrCol
                        + " in on clause is ambiguous ");
              break;
            }
          }

        }

        if (left_table != null && right_table != null)
          throw new SemanticException(
              ErrorMsg.INVALID_JOIN_CONDITION_4.getMsg() + tableOrCol
                  + " in on clause is ambiguous ");

        if (left_table == null && right_table == null)
          throw new SemanticException(
              ErrorMsg.INVALID_JOIN_CONDITION_5.getMsg() + tableOrCol
                  + " in on clause");

        if (left_table != null) {
          if (isPresent(joinTree.getLeftAliases(), left_table.toLowerCase())) {
            if (!leftAliases.contains(left_table.toLowerCase()))
              leftAliases.add(left_table.toLowerCase());
          }
        }

        if (right_table != null) {
          if (isPresent(joinTree.getRightAliases(), right_table.toLowerCase())) {
            if (!rightAliases.contains(right_table.toLowerCase()))
              rightAliases.add(right_table.toLowerCase());
          }
        }
      }

      break;

    case HiveParser.Identifier:
      if (fields != null) {
        fields
            .add(unescapeIdentifier(condn.getToken().getText().toLowerCase()));
      }
      unparseTranslator.addIdentifierTranslation((ASTNode) condn);
      break;

    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      for (int i = 1; i < condn.getChildCount(); i++)
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases, null, qb);
      break;

    default:
      if (condn.getChildCount() == 1)
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),

        leftAliases, rightAliases, null, qb);
      else if (condn.getChildCount() == 2
          || condn.getToken().getText().equalsIgnoreCase("in")) {

        ArrayList<String> fields1 = null;
        if (joinTree.getNoSemiJoin() == false
            && condn.getToken().getType() == HiveParser.DOT) {
          fields1 = new ArrayList<String>();
          int rhssize = rightAliases.size();
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, qb);
          String rhsAlias = null;

          if (rightAliases.size() > rhssize) {
            rhsAlias = rightAliases.get(rightAliases.size() - 1);
          }

          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, qb);
          if (rhsAlias != null && fields1.size() > 0) {
            joinTree.addRHSSemijoinColumns(rhsAlias, condn);
          }
        } else {
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, qb);
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, qb);
        }
      } else
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      break;
    }
  }

  private void populateAliases(Vector<String> leftAliases,
      Vector<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
      Vector<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0))
      throw new SemanticException(
          ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(condn));

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases)
        if (!leftSrc.contains(s))
          leftSrc.add(s);
    } else
      throw new SemanticException(
          ErrorMsg.INVALID_JOIN_CONDITION_2.getMsg(condn));
  }

  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond,
      Vector<String> leftSrc, QB qb) throws SemanticException {
    if (joinCond == null)
      return;

    joinType type = joinTree.getJoinCond()[0].getJoinType();
    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(
          ErrorMsg.INVALID_JOIN_CONDITION_3.getMsg(joinCond));

    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), leftSrc, qb);
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), leftSrc, qb);
      break;

    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      Vector<String> leftCondAl1 = new Vector<String>();
      Vector<String> leftCondAl2 = new Vector<String>();

      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null, qb);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      Vector<String> rightCondAl1 = new Vector<String>();
      Vector<String> rightCondAl2 = new Vector<String>();

      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
          rightCondAl2, null, qb);

      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0)))
        throw new SemanticException(
            ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (leftCondAl1.size() != 0) {
        if ((rightCondAl1.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          if (type.equals(joinType.LEFTOUTER)
              || type.equals(joinType.FULLOUTER)) {
            if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
              joinTree.getFilters().get(0).add(joinCond);
            } else {
              if (!this.hasWith) {
                LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
                joinTree.getFiltersForPushing().get(0).add(joinCond);
              } else {
                boolean cannotPush = false;
                for (String left : leftCondAl1) {
                  if (this.withQueries.contains(left)) {
                    if (this.withCounts.get(left) > 1) {
                      cannotPush = true;
                      break;
                    }
                  }
                }

                if (!cannotPush) {
                  joinTree.getFiltersForPushing().get(0).add(joinCond);
                } else {
                  joinTree.getFilters().get(0).add(joinCond);
                }
              }
            }
          } else {
            if (!this.hasWith) {
              joinTree.getFiltersForPushing().get(0).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (String left : leftCondAl1) {
                if (this.withQueries.contains(left)) {
                  if (this.withCounts.get(left) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(0).add(joinCond);
              } else {
                joinTree.getFilters().get(0).add(joinCond);
              }
            }
          }
        } else if (rightCondAl2.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
        }
      } else if (leftCondAl2.size() != 0) {
        if ((rightCondAl2.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          if (type.equals(joinType.RIGHTOUTER)
              || type.equals(joinType.FULLOUTER)) {
            if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
              joinTree.getFilters().get(1).add(joinCond);
            } else {
              LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
              if (!this.hasWith) {
                joinTree.getFiltersForPushing().get(1).add(joinCond);
              } else {
                boolean cannotPush = false;
                for (String left : leftCondAl2) {
                  if (this.withQueries.contains(left)) {
                    if (this.withCounts.get(left) > 1) {
                      cannotPush = true;
                      break;
                    }
                  }
                }

                if (!cannotPush) {
                  joinTree.getFiltersForPushing().get(1).add(joinCond);
                } else {
                  joinTree.getFilters().get(1).add(joinCond);
                }
              }
            }
          } else {
            if (!this.hasWith) {
              joinTree.getFiltersForPushing().get(1).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (String left : leftCondAl2) {
                if (this.withQueries.contains(left)) {
                  if (this.withCounts.get(left) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(1).add(joinCond);
              } else {
                joinTree.getFilters().get(1).add(joinCond);
              }
            }
          }

        } else if (rightCondAl1.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
        }
      } else if (rightCondAl1.size() != 0) {
        if (type.equals(joinType.LEFTOUTER) || type.equals(joinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            if (!this.hasWith) {
              LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
              joinTree.getFiltersForPushing().get(0).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (String right : rightCondAl1) {
                if (this.withQueries.contains(right)) {
                  if (this.withCounts.get(right) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(0).add(joinCond);
              } else {
                joinTree.getFilters().get(0).add(joinCond);
              }
            }
          }
        } else {
          if (!this.hasWith) {
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          } else {
            boolean cannotPush = false;
            for (String right : rightCondAl1) {
              if (this.withQueries.contains(right)) {
                if (this.withCounts.get(right) > 1) {
                  cannotPush = true;
                  break;
                }
              }
            }

            if (!cannotPush) {
              joinTree.getFiltersForPushing().get(0).add(joinCond);
            } else {
              joinTree.getFilters().get(0).add(joinCond);
            }
          }
        }
      } else {
        if (type.equals(joinType.RIGHTOUTER) || type.equals(joinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            if (!this.hasWith) {
              LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
              joinTree.getFiltersForPushing().get(1).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (String right : rightCondAl2) {
                if (this.withQueries.contains(right)) {
                  if (this.withCounts.get(right) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(1).add(joinCond);
              } else {
                joinTree.getFilters().get(1).add(joinCond);
              }
            }
          }
        } else {
          if (!this.hasWith) {
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          } else {
            boolean cannotPush = false;
            for (String right : rightCondAl2) {
              if (this.withQueries.contains(right)) {
                if (this.withCounts.get(right) > 1) {
                  cannotPush = true;
                  break;
                }
              }
            }

            if (!cannotPush) {
              joinTree.getFiltersForPushing().get(1).add(joinCond);
            } else {
              joinTree.getFilters().get(1).add(joinCond);
            }
          }
        }

      }

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);

      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<Vector<String>> leftAlias = new ArrayList<Vector<String>>(
          joinCond.getChildCount() - childrenBegin);
      ArrayList<Vector<String>> rightAlias = new ArrayList<Vector<String>>(
          joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        Vector<String> left = new Vector<String>();
        Vector<String> right = new Vector<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }

      for (int ci = childrenBegin; ci < joinCond.getChildCount(); ci++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(ci),
            leftAlias.get(ci - childrenBegin),
            rightAlias.get(ci - childrenBegin), null, qb);
      }

      boolean leftAliasNull = true;
      for (Vector<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (Vector<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull)
        throw new SemanticException(
            ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (!leftAliasNull) {
        if (type.equals(joinType.LEFTOUTER) || type.equals(joinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            if (!this.hasWith) {
              LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
              joinTree.getFiltersForPushing().get(0).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (Vector<String> left : leftAlias) {
                if (left.size() != 0) {
                  for (String le : left) {
                    if (this.withCounts.get(le) > 1) {
                      cannotPush = true;
                      break;
                    }
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(0).add(joinCond);
              } else {
                joinTree.getFilters().get(0).add(joinCond);
              }
            }
          }
        } else {
          if (!this.hasWith) {
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          } else {
            boolean cannotPush = false;
            for (Vector<String> left : leftAlias) {
              if (left.size() != 0) {
                for (String le : left) {
                  if (this.withCounts.get(le) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }
            }

            if (!cannotPush) {
              joinTree.getFiltersForPushing().get(0).add(joinCond);
            } else {
              joinTree.getFilters().get(0).add(joinCond);
            }
          }
        }
      } else {
        if (type.equals(joinType.RIGHTOUTER) || type.equals(joinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            if (!this.hasWith) {
              LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
              joinTree.getFiltersForPushing().get(1).add(joinCond);
            } else {
              boolean cannotPush = false;
              for (Vector<String> right : rightAlias) {
                if (right.size() != 0) {
                  for (String ri : right) {
                    if (this.withCounts.get(ri) > 1) {
                      cannotPush = true;
                      break;
                    }
                  }
                }
              }

              if (!cannotPush) {
                joinTree.getFiltersForPushing().get(1).add(joinCond);
              } else {
                joinTree.getFilters().get(1).add(joinCond);
              }
            }
          }
        } else {
          if (!this.hasWith) {
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          } else {
            boolean cannotPush = false;
            for (Vector<String> right : rightAlias) {
              if (right.size() != 0) {
                for (String ri : right) {
                  if (this.withCounts != null
                      && this.withCounts.get(ri) != null
                      && this.withCounts.get(ri) > 1) {
                    cannotPush = true;
                    break;
                  }
                }
              }
            }

            if (!cannotPush) {
              joinTree.getFiltersForPushing().get(1).add(joinCond);
            } else {
              joinTree.getFilters().get(1).add(joinCond);
            }
          }
        }
      }

      break;
    }
  }

  @SuppressWarnings("nls")
  public <T extends Serializable> Operator<T> putOpInsertMap(Operator<T> op,
      RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genHavingPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    ASTNode havingExpr = qb.getParseInfo().getHavingForClause(dest);

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();

    Map<ASTNode, String> exprToColumnAlias = qb.getParseInfo()
        .getAllExprToColumnAlias();
    for (ASTNode astNode : exprToColumnAlias.keySet()) {
      if (inputRR.getExpression(astNode) != null) {
        inputRR.put("", exprToColumnAlias.get(astNode),
            inputRR.getExpression(astNode));
      }
    }
    ASTNode condn = (ASTNode) havingExpr.getChild(0);

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new filterDesc(genExprNodeDesc(condn, inputRR, qb), false),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    return output;
  }

  @SuppressWarnings("nls")
  private Operator genFilterPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
    return genFilterPlan(qb, (ASTNode) whereExpr.getChild(0), input);
  }

  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, ASTNode condn, Operator input)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();

    filterDesc desc = new filterDesc(genExprNodeDesc(condn, inputRR, qb), false);
    Operator output = putOpInsertMap(
        OperatorFactory.getAndMakeChild(desc,
            new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    filterExprString = desc.getPredicate().getExprString();
    LOG.debug("filterExprString: " + filterExprString);

    if (indexQueryInfo.paramMap != null) {
      boolean isIndexMode = parsePredicate(filterExprString, indexQueryInfo);

    }

    LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: "
        + inputRR.toString());
    return output;
  }

  private boolean parsePredicate(String filterExpr,
      IndexQueryInfo indexQueryInfo) {
    if (!canUseIndex) {
      return false;
    }

    filterExpr = filterExpr.trim();
    while (filterExpr.startsWith("("))
      filterExpr = filterExpr.substring(1, filterExpr.length());

    while (filterExpr.endsWith(")"))
      filterExpr = filterExpr.substring(0, filterExpr.length() - 1);

    String expr = filterExpr.toLowerCase();

    if (expr.contains(" or ") || expr.contains("<") || expr.contains("<=")
        || expr.contains(">") || expr.contains(">=") || expr.contains("!=")
        || expr.contains("count(1)") || expr.contains("count(*)")
        || expr.contains(" union ") || expr.contains(" join ")
        || expr.contains("group by") || expr.contains("order by")) {
      return false;
    }

    List<String> fieldList = new ArrayList<String>();
    List<String> fieldValues = new ArrayList<String>();
    String[] conds = expr.split("and");
    if (conds == null) {
      return false;
    }
    for (int i = 0; i < conds.length; i++) {
      conds[i] = conds[i].trim();

      while (conds[i].startsWith("("))
        conds[i] = conds[i].substring(1, conds[i].length());

      while (conds[i].endsWith(")"))
        conds[i] = conds[i].substring(0, conds[i].length() - 1);

      String tmpString = conds[i];

      String[] fieldInfo = tmpString.split("=");

      if (fieldInfo != null && fieldInfo.length == 2) {
        String fieldString = "";
        String valueString = "";

        fieldInfo[0] = fieldInfo[0].trim();
        fieldInfo[1] = fieldInfo[1].trim();
        while (fieldInfo[0].startsWith("("))
          fieldInfo[0] = fieldInfo[0].substring(1, fieldInfo[0].length());
        while (fieldInfo[0].endsWith(")"))
          fieldInfo[0] = fieldInfo[0].substring(0, fieldInfo[0].length() - 1);

        while (fieldInfo[1].startsWith("("))
          fieldInfo[1] = fieldInfo[1].substring(1, fieldInfo[1].length());
        while (fieldInfo[1].endsWith(")"))
          fieldInfo[1] = fieldInfo[1].substring(0, fieldInfo[1].length() - 1);

        if (fieldInfo[0].startsWith("udftodouble")) {
          fieldString = fieldInfo[0].substring("udftodouble".length() + 1)
              .trim();
        } else {
          fieldString = fieldInfo[0].trim();
        }

        if (fieldInfo[1].startsWith("udftodouble")) {
          valueString = fieldInfo[1].substring("udftodouble".length() + 1)
              .trim();
        } else {
          valueString = fieldInfo[1].trim();
        }

        fieldList.add(fieldString);
        fieldValues.add(valueString);
      }
    }
    if (fieldList.isEmpty() || fieldValues.isEmpty()) {
      return false;
    }

    String fieldListString = "";
    List<IndexItem> indexInfo = null;
    Table tab = null;
    try {
      int optimizeLevel = conf.getInt("hive.semantic.analyzer.optimizer.level",
          2);

      String dbName = SessionState.get().getDbName();
      String tblName = indexQueryInfo.tblName;

      tab = getTblFromCache(dbName, tblName, allTableMap, optimizeLevel);

      for (int i = 0; i < fieldList.size(); i++) {
        if (i == 0) {
          fieldListString = getFieldIndxByName(fieldList.get(i), tab.getCols());
        } else {
          fieldListString += ","
              + getFieldIndxByName(fieldList.get(i), tab.getCols());
        }
      }
      if (fieldListString.length() == 0) {
        return false;
      }

      indexInfo = this.db.getAllIndexTable(SessionState.get().getDbName(),
          indexQueryInfo.tblName);
      if (indexInfo == null || indexInfo.isEmpty()) {
        return false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    String indexName = "";
    String indexLocation = "";
    for (int i = 0; i < indexInfo.size(); i++) {
      IndexItem item = indexInfo.get(i);
      if (item.getStatus() != MIndexItem.IndexStatusDone) {
        continue;
      }
      if (item.getFieldList().equals(fieldListString)) {
        indexName = item.getName();
        indexLocation = item.getLocation();
        break;
      }
      if (item.getFieldList().startsWith(fieldListString)) {
        indexName = item.getName();
        indexLocation = item.getLocation();
        break;
      }
    }

    if (indexName.length() == 0) {
      return false;
    }

    if (indexQueryInfo.values == null) {
      indexQueryInfo.values = new ArrayList<IndexValue>();
    }
    getFieldValue(tab.getCols(), fieldListString, fieldValues,
        indexQueryInfo.values);

    indexQueryInfo.indexName = indexName;
    indexQueryInfo.fieldList = fieldListString;
    indexQueryInfo.location = indexLocation;
    indexQueryInfo.isIndexMode = true;

    return true;
  }

  private void getFieldValue(List<FieldSchema> cols, String fieldListString,
      List<String> fieldValues, List<IndexValue> indexValue) {
    String[] fieldIdxs = fieldListString.split(",");
    for (int i = 0; i < fieldIdxs.length; i++) {
      IndexValue values = new IndexValue();
      String type = cols.get(Integer.valueOf(fieldIdxs[i])).getType();

      if (type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {
        values.type = Constants.TINYINT_TYPE_NAME;
        values.value = new Byte(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)) {
        values.type = Constants.SMALLINT_TYPE_NAME;
        values.value = new Short(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
        values.type = Constants.INT_TYPE_NAME;
        values.value = new Integer(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
        values.type = Constants.BIGINT_TYPE_NAME;
        values.value = new Long(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
        values.type = Constants.FLOAT_TYPE_NAME;
        values.value = new Float(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
        values.type = Constants.DOUBLE_TYPE_NAME;
        values.value = new Double(fieldValues.get(i).replace(" ", ""));
      } else if (type.equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
        values.type = Constants.STRING_TYPE_NAME;

        String tmpValue = fieldValues.get(i);
        if (tmpValue.startsWith("'"))
          tmpValue = tmpValue.substring(1, tmpValue.length());
        if (tmpValue.endsWith("'"))
          tmpValue = tmpValue.substring(0, tmpValue.length() - 1);

        values.value = new String(tmpValue);
      } else {
        return;
      }

      indexValue.add(values);
    }
  }

  private String getFieldIndxByName(String name, List<FieldSchema> fields) {
    for (int i = 0; i < fields.size(); i++) {
      if (name.equalsIgnoreCase(fields.get(i).getName())) {
        return "" + i;
      }
    }

    return "" + -1;
  }

  @SuppressWarnings("nls")
  private Integer genColListRegex(String colRegex, String tabAlias,
      String alias, ASTNode sel, ArrayList<exprNodeDesc> col_list,
      RowResolver input, Integer pos, RowResolver output, QB qb)
      throws SemanticException {

    LOG.debug("genColListRegex tabAlias : " + tabAlias + " "
        + sel.toStringTree());
    LOG.debug("alias = " + alias);
    LOG.debug("ASTNode sel: " + sel.toStringTree());
    LOG.debug("pos = " + pos);

    String tableOrCol = null;

    String fullAlias = getFullAlias(qb, sel);

    if (sel.getChildCount() >= 1) {
      tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(
          sel.getChild(0).getText()).toLowerCase();
    }
    if (fullAlias == null) {
      fullAlias = tabAlias;
      LOG.debug("tabAlias is from lateralview : " + fullAlias);
    }

    if (fullAlias != null && !input.hasTableAlias(fullAlias))
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));

    LOG.debug("in genColListRegex tableOrCol: " + tableOrCol);
    LOG.debug("in genColListRegex fullAlias: " + fullAlias);

    Pattern regex = null;
    try {
      regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel,
          e.getMessage()));
    }

    StringBuilder replacementText = new StringBuilder();
    int matched = 0;
    for (ColumnInfo colInfo : input.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String[] tmp = input.reverseLookup(name);

      if (fullAlias != null && !tmp[0].equalsIgnoreCase(fullAlias)) {
        continue;
      }

      if (!regex.matcher(tmp[1]).matches()) {
        continue;
      }

      if (isStarAndAnalysisFunction) {
        if (rowsShouldnotbeShow.contains(colInfo.getInternalName())) {
          LOG.debug("colInfo.getInternalName:  " + colInfo.getInternalName());
          continue;
        }
      }

      LOG.debug("YYY add name:" + name + " " + colInfo.getTabAlias() + " ");
      exprNodeColumnDesc expr = new exprNodeColumnDesc(colInfo.getType(), name,
          colInfo.getTabAlias(), colInfo.getIsPartitionCol());
      col_list.add(expr);
      output.put(tmp[0], tmp[1], new ColumnInfo(getColumnInternalName(pos),
          colInfo.getType(), colInfo.getTabAlias(),
          colInfo.getIsPartitionCol(), tmp[1]));
      pos = Integer.valueOf(pos.intValue() + 1);
      matched++;

      if (unparseTranslator.isEnabled()) {
        if (replacementText.length() > 0) {
          replacementText.append(", ");
        }
        
        String[] tnameTmp = HiveUtils.unparseIdentifier(tmp[0]).split("#");
        String tnew = null;
        if (tnameTmp.length == 2)
          tnew = tnameTmp[1];
        else
          tnew = tnameTmp[0].replace("/", "::");
        replacementText.append(tnew);
        replacementText.append(".");
        replacementText.append(HiveUtils.unparseIdentifier(tmp[1]));
      }
    }

    if (isStarAndAnalysisFunction) {
      isStarAndAnalysisFunction = false;
      rowsShouldnotbeShow.clear();
    }

    if (matched == 0) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel));
    }

    LOG.info("Added to unparseTranslator: " + sel.toStringTree() + "  TO  "
        + replacementText);
    if (unparseTranslator.isEnabled()) {
      unparseTranslator.addTranslation(sel, replacementText.toString());
    }
    return pos;
  }

  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  private String getFixedCmd(String cmd) {
    SessionState ss = SessionState.get();
    if (ss == null)
      return cmd;

    if(ShimLoader.getHadoopShims().isLocalMode(ss.getConf())){
      Set<String> files = ss
          .list_resource(SessionState.ResourceType.FILE, null);
      if ((files != null) && !files.isEmpty()) {
        int end = cmd.indexOf(" ");
        String prog = (end == -1) ? cmd : cmd.substring(0, end);
        String args = (end == -1) ? "" : cmd.substring(end, cmd.length());

        for (String oneFile : files) {
          Path p = new Path(oneFile);
          if (p.getName().equals(prog)) {
            cmd = oneFile + args;
            break;
          }
        }
      }
    }

    return cmd;
  }

  private tableDesc getTableDescFromSerDe(ASTNode child, String cols,
      String colTypes, boolean defaultCols) throws SemanticException {
    if (child.getType() == HiveParser.TOK_SERDENAME) {
      String serdeName = unescapeSQLString(child.getChild(0).getText());
      Class<? extends Deserializer> serdeClass = null;

      try {
        serdeClass = (Class<? extends Deserializer>) Class.forName(serdeName,
            true, JavaUtils.getClassLoader());
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }

      tableDesc tblDesc = PlanUtils.getTableDesc(serdeClass,
          Integer.toString(Utilities.tabCode), cols, colTypes, defaultCols,
          true);
      if (child.getChildCount() == 2) {
        ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1)).getChild(0);
        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
          String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
              .getText());
          String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
              .getText());
          tblDesc.getProperties().setProperty(key, value);
        }
      }
      return tblDesc;
    } else if (child.getType() == HiveParser.TOK_SERDEPROPS) {
      tableDesc tblDesc = PlanUtils.getDefaultTableDesc(
          Integer.toString(Utilities.ctrlaCode), cols, colTypes, defaultCols);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          String fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties()
              .setProperty(Constants.FIELD_DELIM, fieldDelim);
          tblDesc.getProperties().setProperty(Constants.SERIALIZATION_FORMAT,
              fieldDelim);

          if (rowChild.getChildCount() >= 2) {
            String fieldEscape = unescapeSQLString(rowChild.getChild(1)
                .getText());
            tblDesc.getProperties().setProperty(Constants.ESCAPE_CHAR,
                fieldDelim);
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          tblDesc.getProperties().setProperty(Constants.COLLECTION_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          tblDesc.getProperties().setProperty(Constants.MAPKEY_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          tblDesc.getProperties().setProperty(Constants.LINE_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        default:
          assert false;
        }
      }

      return tblDesc;
    }

    return null;
  }

  @SuppressWarnings("nls")
  private Operator genScriptPlan(ASTNode trfm, QB qb, Operator input)
      throws SemanticException {
    ArrayList<ColumnInfo> outputCols = new ArrayList<ColumnInfo>();
    int inputSerDeNum = 1;
    int outputSerDeNum = 3, outputRecordReaderNum = 4;
    int outputColsNum = 5;
    boolean outputColNames = false, outputColSchemas = false;
    int execPos = 2;
    boolean defaultOutputCols = false;

    if (trfm.getChildCount() > outputColsNum) {
      ASTNode outCols = (ASTNode) trfm.getChild(outputColsNum);
      if (outCols.getType() == HiveParser.TOK_ALIASLIST)
        outputColNames = true;
      else if (outCols.getType() == HiveParser.TOK_TABCOLLIST)
        outputColSchemas = true;
    }

    if (!outputColNames && !outputColSchemas) {
      outputCols.add(new ColumnInfo("key", TypeInfoFactory.stringTypeInfo,
          null, false));
      outputCols.add(new ColumnInfo("value", TypeInfoFactory.stringTypeInfo,
          null, false));
      defaultOutputCols = true;
    } else {
      ASTNode collist = (ASTNode) trfm.getChild(outputColsNum);
      int ccount = collist.getChildCount();

      if (outputColNames) {
        for (int i = 0; i < ccount; ++i) {
          outputCols.add(new ColumnInfo(unescapeIdentifier(
              ((ASTNode) collist.getChild(i)).getText()).toLowerCase(),
              TypeInfoFactory.stringTypeInfo, null, false));
        }
      } else {
        for (int i = 0; i < ccount; ++i) {
          ASTNode child = (ASTNode) collist.getChild(i);
          assert child.getType() == HiveParser.TOK_TABCOL;
          outputCols.add(new ColumnInfo(unescapeIdentifier(
              ((ASTNode) child.getChild(0)).getText()).toLowerCase(),
              TypeInfoUtils.getTypeInfoFromTypeString(DDLSemanticAnalyzer
                  .getTypeName(((ASTNode) child.getChild(1)).getType())), null,
              false));
        }
      }
    }

    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();

    for (int i = 0; i < outputCols.size(); ++i) {
      if (i != 0) {
        columns.append(",");
        columnTypes.append(",");
      }

      columns.append(outputCols.get(i).getInternalName());
      columnTypes.append(outputCols.get(i).getType().getTypeName());

      out_rwsch.put(qb.getParseInfo().getAlias(), outputCols.get(i)
          .getInternalName(), outputCols.get(i));
    }

    StringBuilder inpColumns = new StringBuilder();
    StringBuilder inpColumnTypes = new StringBuilder();
    Vector<ColumnInfo> inputSchema = opParseCtx.get(input).getRR()
        .getColumnInfos();
    for (int i = 0; i < inputSchema.size(); ++i) {
      if (i != 0) {
        inpColumns.append(",");
        inpColumnTypes.append(",");
      }

      inpColumns.append(inputSchema.get(i).getInternalName());
      inpColumnTypes.append(inputSchema.get(i).getType().getTypeName());
    }

    tableDesc outInfo;
    tableDesc inInfo;
    String defaultSerdeName = conf.getVar(HiveConf.ConfVars.HIVESCRIPTSERDE);
    Class<? extends Deserializer> serde;

    try {
      serde = (Class<? extends Deserializer>) Class.forName(defaultSerdeName,
          true, JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }

    if (trfm.getChild(inputSerDeNum).getChildCount() > 0)
      inInfo = getTableDescFromSerDe(
          (ASTNode) (((ASTNode) trfm.getChild(inputSerDeNum))).getChild(0),
          inpColumns.toString(), inpColumnTypes.toString(), false);
    else
      inInfo = PlanUtils.getTableDesc(serde,
          Integer.toString(Utilities.tabCode), inpColumns.toString(),
          inpColumnTypes.toString(), false, true);

    if (trfm.getChild(inputSerDeNum).getChildCount() > 0)
      outInfo = getTableDescFromSerDe(
          (ASTNode) (((ASTNode) trfm.getChild(outputSerDeNum))).getChild(0),
          columns.toString(), columnTypes.toString(), false);
    else
      outInfo = PlanUtils.getTableDesc(serde,
          Integer.toString(Utilities.tabCode), columns.toString(),
          columnTypes.toString(), defaultOutputCols);

    Class<? extends RecordReader> outRecordReader = getRecordReader((ASTNode) trfm
        .getChild(outputRecordReaderNum));

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new scriptDesc(
            getFixedCmd(stripQuotes(trfm.getChild(execPos).getText())), inInfo,
            outInfo, outRecordReader),
        new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);

    return output;
  }

  private Class<? extends RecordReader> getRecordReader(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0)
      name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);
    else
      name = unescapeSQLString(node.getChild(0).getText());

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  static List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) {
    if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
      ASTNode selectExprs = parseInfo.getSelForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(selectExprs == null ? 0
          : selectExprs.getChildCount());
      if (selectExprs != null) {
        for (int i = 0; i < selectExprs.getChildCount(); ++i) {
          if (((ASTNode) selectExprs.getChild(i)).getToken().getType() == HiveParser.TOK_HINTLIST) {
            continue;
          }
          ASTNode grpbyExpr = (ASTNode) selectExprs.getChild(i).getChild(0);
          result.add(grpbyExpr);
        }
      }
      return result;
    } else {

      if (parseInfo.getDestContainsGroupbyCubeOrRollupClause(dest)) {
        List<ASTNode> result = new ArrayList<ASTNode>();
        if (parseInfo.getGroupingExprsForClause(dest) != null) {
          result.addAll(parseInfo.getGroupingExprsForClause(dest).values());
        }
        result.addAll(parseInfo.getDestToGroupbyNodes(dest));
        return result;
      }
      ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(grpByExprs == null ? 0
          : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
          result.add(grpbyExpr);
        }
      }
      return result;
    }
  }

  private static String[] getColAlias(ASTNode selExpr, String defaultName,
      RowResolver inputRR, QB qb) throws SemanticException {

    String colAlias = null;
    String tabNameOrAlias = null;
    String[] colRef = new String[2];

    if (selExpr.getChildCount() == 2) {
      colAlias = unescapeIdentifier(selExpr.getChild(1).getText())
          .toLowerCase();
      colRef[0] = tabNameOrAlias;
      colRef[1] = colAlias;

      return colRef;
    }

    ASTNode root = (ASTNode) selExpr.getChild(0);
    if (root.getType() == HiveParser.TOK_TABLE_OR_COL) {
      if (root.getChildCount() == 1) {
        colAlias = root.getChild(0).getText();
        colRef[0] = tabNameOrAlias;
        colRef[1] = colAlias;

      } else {
        throw new SemanticException("err column name : "
            + root.getChild(1).getText() + "::" + root.getChild(0).getText());
      }

      return colRef;
    }

    if (root.getType() == HiveParser.DOT) {
      ASTNode tab = (ASTNode) root.getChild(0);

      int isTab = 0;
      int isAlias = 0;
      int isColumn = 0;

      if (tab.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String tabOrAlias = unescapeIdentifier(tab.getChild(0).getText())
            .toLowerCase();
        String db = null;
        boolean setDb = false;
        if (tab.getChildCount() == 2) {
          db = unescapeIdentifier(tab.getChild(1).getText()).toLowerCase();
          setDb = true;
        } else
          db = SessionState.get().getDbName();

        if (setDb) {
          String fullalias = db + "/" + tabOrAlias;
          if (qb.getUserAliasFromDBTB(fullalias) != null)
            if (qb.getUserAliasFromDBTB(fullalias).size() > 1)
              throw new SemanticException("table : " + tabOrAlias
                  + " has more than one alias : "
                  + qb.getUserAliasFromDBTB(fullalias).get(0) + ","
                  + qb.getUserAliasFromDBTB(fullalias).get(1));
            else
              fullalias = fullalias + "#"
                  + qb.getUserAliasFromDBTB(fullalias).get(0);

          if (inputRR.hasTableAlias(fullalias)) {
            tabNameOrAlias = fullalias;
          }

        } else {
          String default_DB_tab = db + "/" + tabOrAlias;

          if (qb.getUserAliasFromDBTB(default_DB_tab) != null)
            if (qb.getUserAliasFromDBTB(default_DB_tab).size() > 1)
              throw new SemanticException("table : " + tabOrAlias
                  + " has more than one alias : "
                  + qb.getUserAliasFromDBTB(default_DB_tab).get(0) + ","
                  + qb.getUserAliasFromDBTB(default_DB_tab).get(1));
            else
              default_DB_tab = default_DB_tab + "#"
                  + qb.getUserAliasFromDBTB(default_DB_tab).get(0);

          if (inputRR.hasTableAlias(default_DB_tab)
              && qb.getTabAliases().contains(default_DB_tab.toLowerCase())) {
            tabNameOrAlias = default_DB_tab;

            isTab = 1;

          }

          if (qb.existsUserAlias(tabOrAlias)) {
            String fullAliasForUserAlias = null;
            if (qb.getSubqAliases().contains(tabOrAlias.toLowerCase())) {
              fullAliasForUserAlias = tabOrAlias;
            } else
              fullAliasForUserAlias = qb.getTableRefFromUserAlias(tabOrAlias)
                  .getDbName()
                  + "/"
                  + qb.getTableRefFromUserAlias(tabOrAlias).getTblName()
                  + "#"
                  + tabOrAlias;

            if (inputRR.hasTableAlias(fullAliasForUserAlias)) {
              tabNameOrAlias = fullAliasForUserAlias;
            }

            isAlias = 1;
          }

          if (null != inputRR.get(null, tabOrAlias)) {
            isColumn = 1;
          }

          if (isTab + isAlias + isColumn > 1) {
            throw new SemanticException("TDW can't judge name : " + tabOrAlias
                + " is a table name or alias or column!");
          }

        }

      }
      ASTNode col = (ASTNode) root.getChild(1);
      if (col.getType() == HiveParser.Identifier) {
        colAlias = unescapeIdentifier(col.getText()).toLowerCase();
      }
    }

    if (colAlias == null) {
      colAlias = defaultName;
    }

    colRef[0] = tabNameOrAlias;
    colRef[1] = colAlias;

    return colRef;
  }

  private static boolean isRegex(String pattern) {
    for (int i = 0; i < pattern.length(); i++) {
      if (!Character.isLetterOrDigit(pattern.charAt(i))
          && pattern.charAt(i) != '_') {
        return true;
      }
    }
    return false;
  }

  private void genFetchTaskForSelectNoFrom() throws SemanticException {
    if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());

    String cols = loadFileWork.get(0).getColumns();
    String colTypes = loadFileWork.get(0).getColumnTypes();

    RowResolver inputRR = opParseCtx.get(filesinkop).getRR();
    LOG.debug(inputRR.toString());
    HashMap<String, LinkedHashMap<String, ColumnInfo>> columnMap = inputRR
        .rslvMap();
    int index = 0;
    String colNameList = new String("");
    String[] colNameType;
    Map<String, String> colMap = new HashMap<String, String>();

    Vector<ColumnInfo> colsinfo = inputRR.getColumnInfos();
    String newcolNameList = new String("");
    if (colsinfo.size() > 1) {
      newcolNameList = colsinfo.get(0).getAlias();
      for (int i = 1; i < colsinfo.size(); i++) {
        newcolNameList += ",";
        newcolNameList += colsinfo.get(i).getAlias();
      }
      LOG.debug("newcolNameList:  " + newcolNameList);
    } else if (colsinfo.size() == 1) {
      newcolNameList = colsinfo.get(0).getAlias();
      LOG.debug("newcolNameList:  " + newcolNameList);
    }

    for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : columnMap
        .entrySet()) {
      String tab = (String) e.getKey();
      HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>) e
          .getValue();
      if (f_map != null) {
        for (Map.Entry<String, ColumnInfo> entry : f_map.entrySet()) {
          colNameType = entry.getValue().toString().split(":");
          colMap.put(colNameType[0], (String) entry.getKey());
          LOG.debug(entry.getValue().toString());
          LOG.debug((String) entry.getKey());
        }
      }
    }

    int colNum = colMap.size();
    String keyStr = new String("_col");

    for (int i = 0; i < colNum; i++) {
      if (i > 0)
        colNameList += ",";
      colNameList += colMap.get(keyStr + i);

    }

    fetchWork fetch = null;
    int fetchlimit = 1;

    fetch = new fetchWork(
        new Path(loadFileWork.get(0).getSourceDir()).toString(), new tableDesc(
            LazySimpleSerDe.class, MyTextInputFormat.class,
            IgnoreKeyTextOutputFormat.class, Utilities.makeProperties(
                org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                    + Utilities.ctrlaCode,
                org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
                newcolNameList,
                org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
                colTypes)), fetchlimit);

    fetchTask = TaskFactory.get(fetch, this.conf);
    setFetchTask(fetchTask);

    indexQueryInfo.limit = fetchlimit;
  }

  private boolean isSelectWithNoFrom(QB qb) {
    if (qb == null)
      return false;

    QBParseInfo qbf = qb.getParseInfo();

    if (qbf.getIsSubQ()) {
      return false;
    }

    if (!qbf.getIsSelectQuery()) {
      return false;
    }

    if (!qb.getSubqAliases().isEmpty()) {
      return false;
    }

    if (qbf.getHasFromClause()) {
      return false;
    }

    if (qbf.getDestToWhereExpr() == null
        || qbf.getDestToWhereExpr().size() != 0) {
      return false;
    }
    return true;
  }

  private Operator<?> genSelectPlanForNoFrom(String dest, QB qb)
      throws SemanticException {
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);
    RowResolver outRR = new RowResolver();
    RowResolver inputRR = new RowResolver();
    ArrayList<exprNodeDesc> colList = new ArrayList<exprNodeDesc>();

    ASTNode trfm = null;
    String alias = qb.getParseInfo().getAlias();
    Integer pos = Integer.valueOf(0);

    int posn = 0;
    boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
    if (hintPresent) {
      posn++;
    }

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      if (SessionState.get().getUserName().equalsIgnoreCase("root")) {
        trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
      } else {
        throw new SemanticException("only root can do TRANSFORM Operation");
      }
    }

    boolean isUDTF = false;
    String udtfTableAlias = null;
    ArrayList<String> udtfColAliases = new ArrayList<String>();
    ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
    GenericUDTF genericUDTF = null;

    if (udtfExpr.getType() == HiveParser.TOK_FUNCTION) {
      String funcName = TypeCheckProcFactory.DefaultExprProcessor
          .getFunctionText(udtfExpr, true);
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);

      if (fi != null) {
        genericUDTF = fi.getGenericUDTF();
      }

      isUDTF = (genericUDTF != null);

      if (isUDTF && !fi.isNative()) {
        unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
            .getChild(0));
      }
    }

    if (isUDTF) {
      if (selExprList.getChildCount() > 1) {
        throw new SemanticException(ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg());
      }

      ASTNode selExpr = (ASTNode) selExprList.getChild(posn);
      if (selExpr.getChildCount() < 2) {
        throw new SemanticException(ErrorMsg.UDTF_REQUIRE_AS.getMsg());
      }

      for (int i = 1; i < selExpr.getChildCount(); i++) {
        ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
        switch (selExprChild.getType()) {
        case HiveParser.Identifier:
          udtfColAliases.add(unescapeIdentifier(selExprChild.getText()));
          unparseTranslator.addIdentifierTranslation(selExprChild);
          break;

        case HiveParser.TOK_TABALIAS:
          assert (selExprChild.getChildCount() == 1);
          udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
              .getText());
          unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
              .getChild(0));
          break;

        default:
          assert (false);
        }
      }

      LOG.debug("UDTF table alias is " + udtfTableAlias);
      LOG.debug("UDTF col aliases are " + udtfColAliases);
    }

    ASTNode exprList;
    if (isInTransform) {
      exprList = (ASTNode) trfm.getChild(0);
    } else if (isUDTF) {
      exprList = (ASTNode) udtfExpr;
    } else {
      exprList = selExprList;
    }

    int startPosn = isUDTF ? posn + 1 : posn;

    for (int i = startPosn; i < exprList.getChildCount(); ++i) {
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);

      if (!isUDTF && child.getChildCount() > 2) {
        throw new SemanticException(ErrorMsg.INVALID_AS.getMsg());
      }

      ASTNode expr = null;
      String tabAlias = null;
      String colAlias = null;

      if (isInTransform || isUDTF) {
        tabAlias = null;
        colAlias = "_C" + i;
        expr = child;
      } else {
        tabAlias = null;
        colAlias = "_C" + i;

        if (hasAsClause) {
          unparseTranslator.addIdentifierTranslation((ASTNode) child
              .getChild(1));
          colAlias = ((ASTNode) child.getChild(1)).getText();
        }

        expr = (ASTNode) child.getChild(0);
      }

      LOG.debug("XXXgenSelectPlan : expr : " + expr.toStringTree());
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        throw new SemanticException("select * must has a from subquery");
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL) {
        LOG.debug("XXXgenSelectPlan HiveParser.TOK_TABLE_OR_COL ");
        throw new SemanticException("select cols must has a from subquery");
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        LOG.debug("XXXgenSelectPlan HiveParser.DOT.TOK_TABLE_OR_COL ");
        throw new SemanticException("select tbl.cols must has a from subquery");
      } else {
        LOG.debug("EXPR: " + expr.toStringTree());
        exprNodeDesc exp = genExprNodeDesc(expr, inputRR, qb);
        colList.add(exp);

        if (!StringUtils.isEmpty(alias) && (outRR.get(null, colAlias) != null)) {
          if (expr.getChild(1) == null) {
            throw new SemanticException(
                ErrorMsg.AMBIGUOUS_COLUMN.getMsg(colAlias));
          } else {
            throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(expr
                .getChild(1)));
          }
        }

        LOG.debug("XXX put out_rwsch .....tabAlias : " + tabAlias
            + " colAlias : " + colAlias);
        outRR.put(tabAlias, colAlias, new ColumnInfo(
            getColumnInternalName(pos), exp.getTypeInfo(), tabAlias, false));
        LOG.debug("---  " + tabAlias + " " + colAlias + " "
            + exp.getTypeInfo().toString());

        pos = Integer.valueOf(pos.intValue() + 1);
      }
    }

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    for (int i = 0; i < colList.size(); i++) {
      if (colList.get(i) instanceof exprNodeNullDesc) {
        colList.set(i, new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo,
            null));
      }

      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, colList.get(i));
      columnNames.add(outputCol);
    }

    Operator outputOp = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new LocalSelectDesc(colList, columnNames),
        new RowSchema(outRR.getColumnInfos())), outRR);

    outputOp.setColumnExprMap(colExprMap);
    if (isInTransform) {
      outputOp = genScriptPlan(trfm, qb, outputOp);
    }

    if (isUDTF) {
      outputOp = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases, qb,
          outputOp);
    }

    LOG.debug("Created Select Plan row schema: " + outRR.toString());
    return outputOp;
  }

  private Operator<?> genSelectPlan(String dest, QB qb, Operator<?> input)
      throws SemanticException {
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);

    Operator<?> op = genSelectPlan(dest, selExprList, qb, input);
    LOG.debug("XXXCreated Select Plan for clause: " + dest);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator<?> genSelectPlan(String dest, ASTNode selExprList, QB qb,
      Operator<?> input) throws SemanticException {
    LOG.debug("XXX tree: " + selExprList.toStringTree());

    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    ASTNode trfm = null;
    String alias = qb.getParseInfo().getAlias();
    Integer pos = Integer.valueOf(0);
    RowResolver inputRR = opParseCtx.get(input).getRR();
    boolean selectStar = false;
    int posn = 0;
    boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
    if (hintPresent) {
      posn++;
    }

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      if (SessionState.get().getUserName().equalsIgnoreCase("root")) {
        trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
      } else {
        throw new SemanticException("only root can do TRANSFORM Operation");
      }
    }

    boolean isUDTF = false;
    String udtfTableAlias = null;
    ArrayList<String> udtfColAliases = new ArrayList<String>();
    ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
    GenericUDTF genericUDTF = null;

    if (udtfExpr.getType() == HiveParser.TOK_FUNCTION) {
      String funcName = TypeCheckProcFactory.DefaultExprProcessor
          .getFunctionText(udtfExpr, true);
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
      if (fi != null) {
        genericUDTF = fi.getGenericUDTF();
      }
      isUDTF = (genericUDTF != null);
      if (isUDTF && !fi.isNative()) {
        unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
            .getChild(0));
      }
    }

    if (isUDTF) {
      if (selExprList.getChildCount() > 1) {
        throw new SemanticException(ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg());
      }
      ASTNode selExpr = (ASTNode) selExprList.getChild(posn);
      if (selExpr.getChildCount() < 2) {
        throw new SemanticException(ErrorMsg.UDTF_REQUIRE_AS.getMsg());
      }

      for (int i = 1; i < selExpr.getChildCount(); i++) {
        ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
        switch (selExprChild.getType()) {
        case HiveParser.Identifier:
          udtfColAliases.add(unescapeIdentifier(selExprChild.getText()));
          unparseTranslator.addIdentifierTranslation(selExprChild);
          break;
        case HiveParser.TOK_TABALIAS:
          assert (selExprChild.getChildCount() == 1);
          udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
              .getText());
          unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
              .getChild(0));
          break;
        default:
          assert (false);
        }
      }
      LOG.debug("UDTF table alias is " + udtfTableAlias);
      LOG.debug("UDTF col aliases are " + udtfColAliases);
    }

    ASTNode exprList;
    if (isInTransform) {
      exprList = (ASTNode) trfm.getChild(0);
    } else if (isUDTF) {
      exprList = (ASTNode) udtfExpr;
    } else {
      exprList = selExprList;
    }

    LOG.debug("XXXgenSelectPlan: input = " + inputRR.toString());

    int startPosn = isUDTF ? posn + 1 : posn;

    for (int i = startPosn; i < exprList.getChildCount(); ++i) {
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);

      if (!isUDTF && child.getChildCount() > 2) {
        throw new SemanticException(ErrorMsg.INVALID_AS.getMsg());
      }

      ASTNode expr;
      String tabAlias;
      String colAlias;

      if (isInTransform || isUDTF) {
        tabAlias = null;
        colAlias = "_C" + i;
        expr = child;
      } else {
        String[] colRef = getColAlias(child, "_C" + i, inputRR, qb);
        tabAlias = colRef[0];
        colAlias = colRef[1];
        if (hasAsClause) {
          unparseTranslator.addIdentifierTranslation((ASTNode) child
              .getChild(1));
        }
        expr = (ASTNode) child.getChild(0);
      }

      LOG.debug("XXXgenSelectPlan : expr : " + expr.toStringTree());
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        isFixUnionAndSelectStar = true;
        pos = genColListRegex(".*", expr.getChildCount() == 0 ? null
            : unescapeIdentifier(expr.getChild(0).getText().toLowerCase()),
            alias, expr, col_list, inputRR, pos, out_rwsch, qb);
        selectStar = true;
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(0).getText()))) {
        LOG.debug("XXXgenSelectPlan HiveParser.TOK_TABLE_OR_COL ");
        pos = genColListRegex(unescapeIdentifier(expr.getChild(0).getText()),
            null, alias, expr, col_list, inputRR, pos, out_rwsch, qb);
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && inputRR
              .hasTableAlias(getFullAlias(qb, (ASTNode) expr.getChild(0)))
          && !hasAsClause && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(1).getText()))) {
        LOG.debug("XXXgenSelectPlan HiveParser.DOT.TOK_TABLE_OR_COL ");
        pos = genColListRegex(unescapeIdentifier(expr.getChild(1).getText()),
            unescapeIdentifier(expr.getChild(0).getChild(0).getText()
                .toLowerCase()), alias, (ASTNode) (expr.getChild(0)), col_list,
            inputRR, pos, out_rwsch, qb);
      } else {
        LOG.debug("XXXgenSelectPlan HiveParser. last ");

        HashMap<String, ColumnInfo> map = inputRR.getFieldMap(inputRR
            .getTableNames().iterator().next());
        Set<String> cols = map.keySet();

        exprNodeDesc exp = genExprNodeDesc(expr, inputRR, qb);

        col_list.add(exp);

        if (!StringUtils.isEmpty(alias)
            && (out_rwsch.get(null, colAlias) != null)) {
          if (expr.getChild(1) == null) {
            throw new SemanticException(
                ErrorMsg.AMBIGUOUS_COLUMN.getMsg(colAlias));
          } else {
            throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(expr
                .getChild(1)));
          }
        }

        LOG.debug("XXX put out_rwsch .....tabAlias : " + tabAlias
            + " colAlias : " + colAlias);
        out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
            getColumnInternalName(pos), exp.getTypeInfo(), tabAlias, false));
        LOG.debug("---  " + tabAlias + " " + colAlias + " "
            + exp.getTypeInfo().toString());

        pos = Integer.valueOf(pos.intValue() + 1);

        if (isStarAndAnalysisFunction) {
          isStarAndAnalysisFunction = false;
          rowsShouldnotbeShow.clear();
        }
      }
    }
    selectStar = selectStar && exprList.getChildCount() == posn + 1;

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    for (int i = 0; i < col_list.size(); i++) {
      if (col_list.get(i) instanceof exprNodeNullDesc) {
        col_list.set(i, new exprNodeConstantDesc(
            TypeInfoFactory.stringTypeInfo, null));
      }
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, col_list.get(i));
      columnNames.add(outputCol);
    }

    if (selectStar && dest != null
        && qb.getParseInfo().getDestContainsGroupbyCubeOrRollupClause(dest)) {
      throw new SemanticException(
          "select * and cube rollup can not exist together");
    }
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(col_list, columnNames, selectStar), new RowSchema(
            out_rwsch.getColumnInfos()), input), out_rwsch);

    output.setColumnExprMap(colExprMap);
    if (isInTransform) {
      output = genScriptPlan(trfm, qb, output);
    }

    if (isUDTF) {
      output = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases, qb,
          output);
    }

    LOG.debug("Created Select Plan row schema: " + out_rwsch.toString());
    return output;
  }

  static class GenericUDAFInfo {
    ArrayList<exprNodeDesc> convertedParameters;
    GenericUDAFEvaluator genericUDAFEvaluator;
    TypeInfo returnType;
  }

  static class GenericUDWFInfo {
    ArrayList<exprNodeDesc> convertedParameters;
    GenericUDWFEvaluator genericUDWFEvaluator;
    TypeInfo returnType;
  }

  static GenericUDWFEvaluator getGenericUDWFEvaluator(String analysisName,
      ArrayList<exprNodeDesc> analysisParameters, ASTNode analysisTree,
      boolean isDistinct, boolean isOrderBy) throws SemanticException {
    ArrayList<TypeInfo> originalParameterTypeInfos = null;
    if (analysisParameters != null)
      originalParameterTypeInfos = getTypeInfo(analysisParameters);

    GenericUDWFEvaluator result = FunctionRegistry.getGenericUDWFEvaluator(
        analysisName, originalParameterTypeInfos, isDistinct, isOrderBy);
    if (null == result) {
      String reason = "Looking for UDWF Evaluator\"" + analysisName
          + "\" with parameters " + originalParameterTypeInfos;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
          (ASTNode) analysisTree.getChild(0), reason));
    }
    return result;
  }

  static GenericUDWFInfo getGenericUDWFInfo(GenericUDWFEvaluator evaluator,
      ArrayList<exprNodeDesc> analysisParameters, boolean isDistinct,
      boolean isOrderBy) throws SemanticException {

    GenericUDWFInfo r = new GenericUDWFInfo();

    r.genericUDWFEvaluator = evaluator;

    ObjectInspector returnOI = null;
    try {
      ObjectInspector[] analysisObjectInspectors = null;

      if (analysisParameters != null)
        analysisObjectInspectors = getStandardObjectInspector(getTypeInfo(analysisParameters));

      returnOI = r.genericUDWFEvaluator.init(analysisObjectInspectors);
      r.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    r.convertedParameters = analysisParameters;

    return r;
  }

  static ArrayList<TypeInfo> getTypeInfo(ArrayList<exprNodeDesc> exprs) {
    ArrayList<TypeInfo> result = new ArrayList<TypeInfo>();
    for (exprNodeDesc expr : exprs) {
      result.add(expr.getTypeInfo());
    }
    return result;
  }

  static ObjectInspector[] getStandardObjectInspector(ArrayList<TypeInfo> exprs) {
    ObjectInspector[] result = new ObjectInspector[exprs.size()];
    for (int i = 0; i < exprs.size(); i++) {
      result[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(exprs.get(i));
    }
    return result;
  }

  static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName,
      ArrayList<exprNodeDesc> aggParameters, ASTNode aggTree,
      boolean isDistinct, boolean isAllColumns) throws SemanticException {
    ArrayList<TypeInfo> originalParameterTypeInfos = getTypeInfo(aggParameters);
    GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
        aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
    if (null == result) {
      String reason = "Looking for UDAF Evaluator\"" + aggName
          + "\" with parameters " + originalParameterTypeInfos;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
          (ASTNode) aggTree.getChild(0), reason));
    }
    return result;
  }

  static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode emode, ArrayList<exprNodeDesc> aggParameters)
      throws SemanticException {

    GenericUDAFInfo r = new GenericUDAFInfo();

    r.genericUDAFEvaluator = evaluator;

    ObjectInspector returnOI = null;
    try {
      ObjectInspector[] aggObjectInspectors = getStandardObjectInspector(getTypeInfo(aggParameters));
      returnOI = r.genericUDAFEvaluator.init(emode, aggObjectInspectors);
      r.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    r.convertedParameters = aggParameters;

    return r;
  }

  static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(
      groupByDesc.Mode mode, boolean isDistinct) {
    switch (mode) {
    case COMPLETE:
      return GenericUDAFEvaluator.Mode.COMPLETE;
    case PARTIAL1:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case PARTIAL2:
      return GenericUDAFEvaluator.Mode.PARTIAL2;
    case PARTIALS:
      return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1
          : GenericUDAFEvaluator.Mode.PARTIAL2;
    case FINAL:
      return GenericUDAFEvaluator.Mode.FINAL;
    case HASH:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case MERGEPARTIAL:
      return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE
          : GenericUDAFEvaluator.Mode.FINAL;
    default:
      throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
    }
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, groupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver.get("", text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), "", false));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", grpbyExpr.toStringTree(),
          new ColumnInfo(field, exprInfo.getType(), null, false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    String lastKeyColName = null;
    if (reduceSinkOperatorInfo.getConf() instanceof reduceSinkDesc) {
      List<String> inputKeyCols = ((reduceSinkDesc) reduceSinkOperatorInfo
          .getConf()).getOutputKeyColumnNames();
      if (inputKeyCols.size() > 0) {
        lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
      }
    }
    int numDistinctUDFs = 0;

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      String aggName = value.getChild(0).getText();
      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      for (int i = 1; i < value.getChildCount(); i++) {
        String text = value.getChild(i).toStringTree();
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ColumnInfo paraExprInfo = groupByInputRowResolver.get("", text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        if (isDistinct && lastKeyColName != null) {
          paraExpression = Utilities.ReduceField.KEY.name() + "."
              + lastKeyColName + ":" + numDistinctUDFs + "."
              + getColumnInternalName(i - 1);

        }

        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
                .getIsPartitionCol()));
      }

      if (isDistinct) {
        numDistinctUDFs++;
      }
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", value.toStringTree(), new ColumnInfo(
          field, udaf.returnType, "", false));
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        reduceSinkOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, groupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      boolean distPartAgg) throws SemanticException {
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); i++) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver.get("", text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), exprInfo.getTabAlias(), exprInfo
          .getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", grpbyExpr.toStringTree(),
          new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    String lastKeyColName = null;
    if (reduceSinkOperatorInfo.getConf() instanceof reduceSinkDesc) {
      List<String> inputKeyCols = ((reduceSinkDesc) reduceSinkOperatorInfo
          .getConf()).getOutputKeyColumnNames();
      if (inputKeyCols.size() > 0) {
        lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
      }
    }
    int numDistinctUDFs = 0;

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = value.getChild(0).getText();
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      boolean isDistinct = (value.getType() == HiveParser.TOK_FUNCTIONDI);

      boolean partialAggDone = !(distPartAgg || isDistinct);
      if (!partialAggDone) {
        for (int i = 1; i < value.getChildCount(); i++) {
          String text = value.getChild(i).toStringTree();
          ASTNode paraExpr = (ASTNode) value.getChild(i);
          ColumnInfo paraExprInfo = groupByInputRowResolver.get("", text);
          if (paraExprInfo == null) {
            throw new SemanticException(
                ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);

          if (isDistinct && lastKeyColName != null) {
            paraExpression = Utilities.ReduceField.KEY.name() + "."
                + lastKeyColName + ":" + numDistinctUDFs + "."
                + getColumnInternalName(i - 1);
          }

          aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(),
              paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
                  .getIsPartitionCol()));
        }
      } else {
        String text = entry.getKey();
        ColumnInfo paraExprInfo = groupByInputRowResolver.get("", text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
        }
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
                .getIsPartitionCol()));
      }
      if (isDistinct) {
        numDistinctUDFs++;
      }
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = null;
      if (distPartAgg) {
        genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters,
            value, isDistinct, isAllColumns);
        assert (genericUDAFEvaluator != null);
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      } else {
        genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
        assert (genericUDAFEvaluator != null);
      }

      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters,
          (mode != groupByDesc.Mode.FINAL && isDistinct), amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", value.toStringTree(), new ColumnInfo(
          field, udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            distPartAgg),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        reduceSinkOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb, String dest,
      Operator inputOperatorInfo, groupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr,
          groupByInputRowResolver, qb);

      groupByKeys.add(grpByExprNode);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr, new ColumnInfo(field,
          grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> list = parseInfo.getDistinctFuncExprsForClause(dest);

      int numDistn = 0;
      for (ASTNode value : list) {
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          if (groupByOutputRowResolver.getExpression(parameter) == null) {
            exprNodeDesc distExprNode = genExprNodeDesc(parameter,
                groupByInputRowResolver, qb);
            groupByKeys.add(distExprNode);
            numDistn++;
            String field = getColumnInternalName(grpByExprs.size() + numDistn
                - 1);
            outputColumnNames.add(field);
            groupByOutputRowResolver.putExpression(parameter, new ColumnInfo(
                field, distExprNode.getTypeInfo(), "", false));
            colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
          }
        }
      }
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = value.getChild(0).getText();
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ArrayList<Class<?>> aggClasses = new ArrayList<Class<?>>();
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        exprNodeDesc paraExprNode = genExprNodeDesc(paraExpr,
            groupByInputRowResolver, qb);

        aggParameters.add(paraExprNode);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);

      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", value.toStringTree(), new ColumnInfo(
          field, udaf.returnType, "", false));
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator(QB qb, String dest,
      Operator inputOperatorInfo, int numPartitionFields, int numReducers,
      boolean mapAggrDone, boolean partitionByGbkeyanddistinctkey)
      throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();

    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); i++) {

      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc inputExpr = genExprNodeDesc(grpbyExpr,
          reduceSinkInputRowResolver, qb);
      reduceKeys.add(inputExpr);
      if (reduceSinkOutputRowResolver.getExpression(grpbyExpr) == null
          || parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
        outputKeyColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), null, false);
        reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
        colExprMap.put(colInfo.getInternalName(), inputExpr);
      } else {
        throw new SemanticException(
            ErrorMsg.DUPLICATE_GROUPBY_KEY.getMsg(grpbyExpr));
      }

    }

    List<List<Integer>> distinctColIndices = new ArrayList<List<Integer>>();
    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> distFuncs = parseInfo.getDistinctFuncExprsForClause(dest);
      String colName = getColumnInternalName(reduceKeys.size());
      for (int i = 0; i < distFuncs.size(); i++) {
        ASTNode value = distFuncs.get(i);
        int numExprs = 0;
        List<Integer> distinctIndices = new ArrayList<Integer>();
        for (int j = 1; j < value.getChildCount(); j++) {
          ASTNode parameter = (ASTNode) value.getChild(j);
          exprNodeDesc expr = genExprNodeDesc(parameter,
              reduceSinkInputRowResolver, qb);
          int ri;
          boolean isgbykey = false;
          for (ri = 0; ri < grpByExprs.size(); ri++) {
            if (reduceKeys.get(ri).getExprString().equals(expr.getExprString())) {
              isgbykey = true;
              break;
            }
          }
          for (ri = grpByExprs.size(); ri < reduceKeys.size(); ri++) {
            if (reduceKeys.get(ri).getExprString().equals(expr.getExprString())) {
              break;
            }
          }
          distinctIndices.add(ri);
          if (ri == reduceKeys.size()) {
            reduceKeys.add(expr);
          }
          String name = getColumnInternalName(numExprs);
          String field = Utilities.ReduceField.KEY.toString() + "." + colName
              + ":" + i + "." + name;
          ColumnInfo colInfo = new ColumnInfo(field, expr.getTypeInfo(), null,
              false);
          if (isgbykey)
            reduceSinkOutputRowResolver.putExpression(parameter, colInfo,
                String.valueOf(i));
          else
            reduceSinkOutputRowResolver.putExpression(parameter, colInfo);
          numExprs++;
        }
        distinctColIndices.add(distinctIndices);
      }
      outputKeyColumnNames.add(colName);
    }

    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          String text = parameter.toStringTree();
          if (reduceSinkOutputRowResolver.get("", text) == null) {
            reduceValues.add(genExprNodeDesc(parameter,
                reduceSinkInputRowResolver, qb));
            outputValueColumnNames.add(getColumnInternalName(reduceValues
                .size() - 1));
            String field = Utilities.ReduceField.VALUE.toString() + "."
                + getColumnInternalName(reduceValues.size() - 1);
            reduceSinkOutputRowResolver.put("", text, new ColumnInfo(field,
                reduceValues.get(reduceValues.size() - 1).getTypeInfo(), null,
                false));
          }
        }
      }
    } else {

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {

        TypeInfo type = reduceSinkInputRowResolver.getExpression(
            entry.getValue()).getType();
        reduceValues.add(new exprNodeColumnDesc(type,
            reduceSinkInputRowResolver.getExpression(entry.getValue())
                .getInternalName(), "", false));
        outputValueColumnNames
            .add(getColumnInternalName(reduceValues.size() - 1));
        String field = Utilities.ReduceField.VALUE.toString() + "."
            + getColumnInternalName(reduceValues.size() - 1);
        reduceSinkOutputRowResolver.put("", ((ASTNode) entry.getValue())
            .toStringTree(), new ColumnInfo(field, type, null, false));
      }
    }

    LOG.debug("partitionByGbkeyanddistinctkey:\t"
        + partitionByGbkeyanddistinctkey);
    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            grpByExprs.size(), reduceValues, distinctColIndices,
            outputKeyColumnNames, outputValueColumnNames, true, -1,
            numPartitionFields, numReducers, partitionByGbkeyanddistinctkey),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
            inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);

    return rsOp;
  }

  private Operator genAnalysisPlanReduceSinkOperator(QB qb, String dest,
      Operator inputOperatorInfo, List<ASTNode> partitionByExprs,
      List<ASTNode> orderByExprs, List<ASTNode> distinctExprs, int numReducers)
      throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(false);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> partitionCols = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    StringBuilder order = new StringBuilder();

    if (partitionByExprs.size() > 0) {
      for (int i = 0; i < partitionByExprs.size(); i++) {
        ASTNode partitionByExpr = partitionByExprs.get(i);
        exprNodeDesc inputExpr = genExprNodeDesc(partitionByExpr,
            reduceSinkInputRowResolver, qb);
        reduceKeys.add(inputExpr);
        partitionCols.add(inputExpr);

        String text = partitionByExpr.toStringTree();
        if (reduceSinkOutputRowResolver.get("", text) == null) {
          outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));

          String field = Utilities.ReduceField.KEY.toString() + "."
              + getColumnInternalName(reduceKeys.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
              reduceKeys.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.put("", text, colInfo);
          colExprMap.put(colInfo.getInternalName(), inputExpr);

          order.append("+");
        } else {
          throw new SemanticException(
              ErrorMsg.DUPLICATE_PARTITIONBY_KEY.getMsg(partitionByExpr));
        }
      }

    } else {
      if (distinctExprs == null) {
        exprNodeDesc constantExpr = new exprNodeConstantDesc(
            TypeInfoFactory.intTypeInfo, 1);
        reduceKeys.add(constantExpr);
        partitionCols.add(constantExpr);

        String text = constantExpr.toString();
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.put("", text, colInfo);
        colExprMap.put(colInfo.getInternalName(), constantExpr);

        order.append("+");
      }
    }

    if (distinctExprs != null) {
      ASTNode parameters = (ASTNode) (distinctExprs.get(0)).getChild(1);

      if (parameters.getChildCount() != 1)
        throw new SemanticException(
            "Only one parameter is allowed in analysis function with distinct");

      ASTNode parameter = (ASTNode) parameters.getChild(0);

      String text = parameter.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        reduceKeys.add(genExprNodeDesc(parameter, reduceSinkInputRowResolver,
            qb));

        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.put("", text, colInfo);
        colExprMap.put(colInfo.getInternalName(),
            reduceKeys.get(reduceKeys.size() - 1));

        order.append("+");
      }
    }

    if (orderByExprs != null) {
      int ccount = orderByExprs.size();

      for (int i = 0; i < ccount; i++) {
        ASTNode orderByExpr = orderByExprs.get(i);

        if (orderByExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          order.append("+");
          orderByExpr = (ASTNode) orderByExpr.getChild(0);
        } else if (orderByExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          order.append("-");
          orderByExpr = (ASTNode) orderByExpr.getChild(0);
        } else
          throw new SemanticException(
              ErrorMsg.ORDER_BY_WITH_WRONG_PARAMETERS.getMsg(orderByExpr));

        String text = orderByExpr.toStringTree();
        if (reduceSinkOutputRowResolver.get("", text) == null) {
          exprNodeDesc exprNode = genExprNodeDesc(orderByExpr,
              reduceSinkInputRowResolver, qb);
          reduceKeys.add(exprNode);

          outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
          String field = Utilities.ReduceField.KEY.toString() + "."
              + getColumnInternalName(reduceKeys.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
              reduceKeys.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.put("", text, colInfo);
          colExprMap.put(colInfo.getInternalName(),
              reduceKeys.get(reduceKeys.size() - 1));
        }
      }
    }

    HashMap<String, ASTNode> analysisTrees = parseInfo
        .getAnalysisExprsForClause(dest);

    for (Map.Entry<String, ASTNode> entry : analysisTrees.entrySet()) {
      ASTNode analysisTree = entry.getValue();

      if (analysisTree.getChild(1).getType() == HiveParser.TOK_FUNCPARAMETER) {
        ASTNode parameters = (ASTNode) analysisTree.getChild(1);

        ASTNode parameter = (ASTNode) parameters.getChild(0);

        String text = parameter.toStringTree();
        if (reduceSinkOutputRowResolver.get("", text) == null) {
          exprNodeDesc exprNode = genExprNodeDesc(parameter,
              reduceSinkInputRowResolver, qb);
          reduceValues.add(exprNode);

          outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
              reduceValues.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.put("", text, colInfo);
          colExprMap.put(colInfo.getInternalName(),
              reduceValues.get(reduceValues.size() - 1));
        }
      }
    }

    Iterator<String> tableNames = reduceSinkInputRowResolver.getTableNames()
        .iterator();

    while (tableNames.hasNext()) {
      String tableName = tableNames.next();
      HashMap<String, ColumnInfo> fieldMap = reduceSinkInputRowResolver
          .getFieldMap(tableName);
      for (Map.Entry<String, ColumnInfo> entry : fieldMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();

        String column = valueInfo.getInternalName();

        if (reduceSinkOutputRowResolver.get(tableName, field) == null) {
          exprNodeColumnDesc inputExpr = new exprNodeColumnDesc(
              valueInfo.getType(), valueInfo.getInternalName(),
              valueInfo.getTabAlias(), valueInfo.getIsPartitionCol());
          reduceValues.add(inputExpr);

          String col = getColumnInternalName(reduceValues.size() - 1);
          outputColumnNames.add(col);
          ColumnInfo newColInfo = new ColumnInfo(
              Utilities.ReduceField.VALUE.toString() + "." + col,
              valueInfo.getType(), tableName, false);
          colExprMap.put(newColInfo.getInternalName(), inputExpr);
          reduceSinkOutputRowResolver.put(tableName, field, newColInfo);
        }
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, partitionCols,
            order.toString(), numReducers), new RowSchema(
            reduceSinkOutputRowResolver.getColumnInfos()), inputOperatorInfo),
        reduceSinkOutputRowResolver);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  @SuppressWarnings("nls")
  private Operator genAnalysisPlanAnalysisOperator(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, List<ASTNode> analysisExprs)
      throws SemanticException {
    RowResolver analysisInputRowResolver = opParseCtx.get(
        reduceSinkOperatorInfo).getRR();
    RowResolver analysisOutputRowResolver = new RowResolver();
    analysisOutputRowResolver.setIsExprResolver(true);

    ArrayList<exprNodeDesc> partitionByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> orderByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<analysisEvaluatorDesc> analysises = new ArrayList<analysisEvaluatorDesc>();
    ArrayList<exprNodeDesc> otherColumns = new ArrayList<exprNodeDesc>();

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    isStarAndAnalysisFunction = true;

    ASTNode functionOverExpr = analysisExprs.get(0);
    List<ASTNode> partitionByExprs = getPartitionByExprs(functionOverExpr);

    int outputpos = 0;

    List<ASTNode> orderByExprs = null;
    for (int i = 0; i < analysisExprs.size(); i++) {
      orderByExprs = getOrderByExprs(analysisExprs.get(i));

      if (orderByExprs != null)
        break;
    }

    if (partitionByExprs.size() > 0) {
      for (int i = 0; i < partitionByExprs.size(); i++) {
        ASTNode partitionByExpr = partitionByExprs.get(i);
        String text = partitionByExpr.toStringTree();
        ColumnInfo exprInfo = analysisInputRowResolver.get("", text);

        if (exprInfo == null) {
          throw new SemanticException(
              ErrorMsg.INVALID_COLUMN.getMsg(partitionByExpr));
        }

        partitionByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo
            .getInternalName(), "", false));

        String field = getColumnInternalName(outputpos++);
        outputColumnNames.add(field);

        rowsShouldnotbeShow.add(field);

        analysisOutputRowResolver.put("", partitionByExpr.toStringTree(),
            new ColumnInfo(field, exprInfo.getType(), "", false));
        colExprMap.put(field, partitionByKeys.get(partitionByKeys.size() - 1));
      }
    }

    if (orderByExprs != null) {
      int ccount = orderByExprs.size();

      for (int i = 0; i < ccount; i++) {
        ASTNode orderByExpr = orderByExprs.get(i);

        if (orderByExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          orderByExpr = (ASTNode) orderByExpr.getChild(0);
        } else if (orderByExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          orderByExpr = (ASTNode) orderByExpr.getChild(0);
        } else
          throw new SemanticException("Something wrong with order by clause!");

        String text = orderByExpr.toStringTree();
        ColumnInfo exprInfo = analysisInputRowResolver.get("", text);

        if (exprInfo == null) {
          throw new SemanticException(
              ErrorMsg.INVALID_COLUMN.getMsg(orderByExpr));
        }

        orderByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), exprInfo
            .getInternalName(), "", false));

        String field = getColumnInternalName(outputpos++);
        outputColumnNames.add(field);

        rowsShouldnotbeShow.add(field);

        analysisOutputRowResolver.put("", orderByExpr.toStringTree(),
            new ColumnInfo(field, exprInfo.getType(), "", false));

        colExprMap.put(field, orderByKeys.get(orderByKeys.size() - 1));
      }
    }

    HashMap<String, ASTNode> analysisTrees = parseInfo
        .getAnalysisExprsForClause(dest);
    assert (analysisTrees != null);

    for (Map.Entry<String, ASTNode> entry : analysisTrees.entrySet()) {
      ASTNode value = entry.getValue();
      int childCount = value.getChildCount();
      assert childCount >= 2;

      String analysisName = value.getChild(0).getText();

      ArrayList<exprNodeDesc> analysisParameters = new ArrayList<exprNodeDesc>();
      boolean isDistinct = false;
      boolean isOrderBy = false;

      ASTNode nodeN = (ASTNode) value.getChild(childCount - 1);
      if (nodeN.getType() == HiveParser.TOK_ORDERBY)
        isOrderBy = true;
      System.out.println(value.toStringTree() + "\t" + isOrderBy);

      ASTNode node1 = (ASTNode) value.getChild(1);
      if (node1.getToken().getType() == HiveParser.TOK_FUNCPARAMETER) {
        int parameterCount = node1.getChildCount();
        LOG.debug("parameterCount: " + parameterCount);
        if (analysisName.equalsIgnoreCase("RANKOVER")
            || analysisName.equalsIgnoreCase("DENSE_RANKOVER")
            || analysisName.equalsIgnoreCase("ROW_NUMBEROVER"))
          throw new SemanticException(
              "Parameters are not allowed in the analysis function "
                  + analysisName.toUpperCase() + "!");

        String text = node1.getChild(0).toStringTree();
        ASTNode paraExpr = (ASTNode) node1.getChild(0);

        ColumnInfo paraExprInfo = analysisInputRowResolver.get("", text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);

        analysisParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(),
            paraExprInfo.getInternalName(), null, false));

        if ((parameterCount == 2 || parameterCount == 3)) {
          if ((analysisName.equalsIgnoreCase("LAGOVER") || analysisName
              .equalsIgnoreCase("LEADOVER"))) {
            Integer v = null;
            ASTNode expr1 = (ASTNode) node1.getChild(1);
            try {
              v = Integer.valueOf(expr1.getText());
            } catch (NumberFormatException e) {
            }
            if (v == null) {
              throw new SemanticException(
                  ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr1));
            }
            if (v.intValue() <= 0) {
              throw new SemanticException(
                  "The 2nd parameter of LAG or LEAD should be a Integer larger than 0!");
            }

            analysisParameters.add(new exprNodeConstantDesc(v));

            if (parameterCount == 3) {
              ASTNode expr2 = (ASTNode) node1.getChild(2);

              String type = paraExprInfo.getType().getTypeName();
              String textValue = expr2.getText();
              Object val = null;

              if (type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {

                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Byte(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Short(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Integer(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Long(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Float(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Double(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  val = new Boolean(textValue.replace(" ", ""));
                }
              } else if (type.equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
                if (textValue.equalsIgnoreCase("TOK_NULL")) {
                  val = null;
                } else {
                  String tmpValue = textValue;

                  if (tmpValue.startsWith("'"))
                    tmpValue = tmpValue.substring(1, tmpValue.length());
                  if (tmpValue.endsWith("'"))
                    tmpValue = tmpValue.substring(0, tmpValue.length() - 1);

                  if (tmpValue.startsWith("\""))
                    tmpValue = tmpValue.substring(1, tmpValue.length());
                  if (tmpValue.endsWith("\""))
                    tmpValue = tmpValue.substring(0, tmpValue.length() - 1);

                  val = new String(tmpValue);
                }
              } else {
                throw new SemanticException(
                    "The 3rd parameter of analysis function is in wrong type!");
              }

              exprNodeConstantDesc parameter3 = new exprNodeConstantDesc(
                  paraExprInfo.getType(), val);
              analysisParameters.add(parameter3);

            }

          } else {
            throw new SemanticException(
                "Too many parameters in the analysis function "
                    + analysisName.toUpperCase() + "!");
          }
        } else if ((parameterCount == 1)) {
          if (analysisName.equalsIgnoreCase("SUMOVER")
              || analysisName.equalsIgnoreCase("AVGOVER")
              || analysisName.equalsIgnoreCase("MAXOVER")
              || analysisName.equalsIgnoreCase("MINOVER")
              || analysisName.equalsIgnoreCase("RATIO_TO_REPORTOVER")) {
            String type = paraExprInfo.getType().getTypeName();

            if (!(type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)
                || type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)
                || type.equalsIgnoreCase(Constants.INT_TYPE_NAME)
                || type.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)
                || type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME) || type
                  .equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)))
              throw new SemanticException("Can not compute on " + type
                  + " type with the " + analysisName + " analysis function!");
          }

        }

        isDistinct = value.getType() == HiveParser.TOK_FUNCTIONOVERDI;
      } else {
        isDistinct = false;
      }

      GenericUDWFEvaluator genericUDWFEvaluator = getGenericUDWFEvaluator(
          analysisName, analysisParameters, value, isDistinct, isOrderBy);
      assert (genericUDWFEvaluator != null);

      GenericUDWFInfo udwf = getGenericUDWFInfo(genericUDWFEvaluator,
          analysisParameters, isDistinct, isOrderBy);
      analysises.add(new analysisEvaluatorDesc(analysisName.toLowerCase(),
          udwf.genericUDWFEvaluator, udwf.convertedParameters, partitionByKeys,
          isDistinct, orderByKeys, isOrderBy));

      String field = getColumnInternalName(outputpos++);
      outputColumnNames.add(field);

      rowsShouldnotbeShow.add(field);

      ColumnInfo newColInfo = new ColumnInfo(field, udwf.returnType, "", false);

      analysisOutputRowResolver.put("", value.toStringTree(), newColInfo);

    }

    HashMap<String, LinkedHashMap<String, ColumnInfo>> all = analysisInputRowResolver
        .rslvMap();
    Iterator<String> tableNames = analysisInputRowResolver.getTableNames()
        .iterator();
    int i = 0;
    while (tableNames.hasNext()) {
      String tableName = tableNames.next();
      HashMap<String, ColumnInfo> fieldMap = all.get(tableName);
      for (Map.Entry<String, ColumnInfo> entry : fieldMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();

        if (analysisOutputRowResolver.get(tableName, field) == null) {
          exprNodeDesc inputExpr = new exprNodeColumnDesc(valueInfo.getType(),
              valueInfo.getInternalName(), valueInfo.getTabAlias(),
              valueInfo.getIsPartitionCol());
          otherColumns.add(inputExpr);

          String col = getColumnInternalName(outputpos++);
          outputColumnNames.add(col);

          if (field.equalsIgnoreCase("const int 1")) {
            rowsShouldnotbeShow.add(col);
          }
          if (field.toString().contains("tok_table_or_col")) {
            rowsShouldnotbeShow.add(col);
          }

          ColumnInfo newColInfo = new ColumnInfo(col, valueInfo.getType(),
              tableName, false);

          analysisOutputRowResolver.put(tableName, field, newColInfo);
          colExprMap.put(newColInfo.getInternalName(),
              otherColumns.get(otherColumns.size() - 1));
          i++;
        }
      }
    }

    Operator op = putOpInsertMap(
        OperatorFactory.getAndMakeChild(new analysisDesc(outputColumnNames,
            partitionByKeys, orderByKeys, false, analysises, otherColumns),
            new RowSchema(analysisOutputRowResolver.getColumnInfos()),
            reduceSinkOperatorInfo), analysisOutputRowResolver);

    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(QBParseInfo parseInfo,
      String dest, Operator groupByOperatorInfo, int numPartitionFields,
      int numReducers) throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(
        groupByOperatorInfo).getRR();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("",
          grpbyExpr.toStringTree()).getType();
      exprNodeColumnDesc inputExpr = new exprNodeColumnDesc(typeInfo, field,
          "", false);
      reduceKeys.add(inputExpr);
      ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString()
          + "." + field, typeInfo, "", false);
      reduceSinkOutputRowResolver2.put("", grpbyExpr.toStringTree(), colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      String field = getColumnInternalName(inputField);
      ASTNode t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("", t.toStringTree())
          .getType();
      reduceValues.add(new exprNodeColumnDesc(typeInfo, field, "", false));
      inputField++;
      String col = getColumnInternalName(reduceValues.size() - 1);
      outputColumnNames.add(col);
      reduceSinkOutputRowResolver2.put("", t.toStringTree(), new ColumnInfo(
          Utilities.ReduceField.VALUE.toString() + "." + col, typeInfo, "",
          false));
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, numPartitionFields,
            numReducers),
            new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()),
            groupByOperatorInfo), reduceSinkOutputRowResolver2);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo2, groupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver2 = opParseCtx.get(
        reduceSinkOperatorInfo2).getRR();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver2.get("", text);
      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), expression,
          exprInfo.getTabAlias(), exprInfo.getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.put("", grpbyExpr.toStringTree(),
          new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ASTNode value = entry.getValue();
      String text = entry.getKey();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.get("", text);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert (paraExpression != null);
      aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(),
          paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
              .getIsPartitionCol()));

      String aggName = value.getChild(0).getText();

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isStar = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
          .get(entry.getKey());
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations
          .add(new aggregationDesc(
              aggName.toLowerCase(),
              udaf.genericUDAFEvaluator,
              udaf.convertedParameters,
              (mode != groupByDesc.Mode.FINAL && value.getToken().getType() == HiveParser.TOK_FUNCTIONDI),
              amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.put("", value.toStringTree(), new ColumnInfo(
          field, udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false), new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
        reduceSinkOperatorInfo2), groupByOutputRowResolver2);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings({ "unused", "nls" })
  private Operator genGroupByPlan1MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, input, grpByExprs.size(), numReducers, false, false);

    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.COMPLETE, null);

    return groupByOperatorInfo;
  }

  private Operator genAnalysisPlan1MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;

    Collection<ASTNode> analysisNodes = parseInfo.getAnalysisExprsForClause(
        dest).values();
    List<ASTNode> analysisExprs = new ArrayList(analysisNodes == null ? 0
        : analysisNodes.size());
    if (analysisNodes != null) {
      for (ASTNode analysisNode : analysisNodes) {
        analysisExprs.add(analysisNode);
      }
    }

    assert analysisExprs.size() != 0;

    ASTNode functionOverExpr = analysisExprs.get(0);
    List<ASTNode> partitionByExprs = getPartitionByExprs(functionOverExpr);

    List<ASTNode> orderByExprs = null;
    for (int i = 0; i < analysisExprs.size(); i++) {
      orderByExprs = getOrderByExprs(analysisExprs.get(i));

      if (orderByExprs != null)
        break;
    }

    List<ASTNode> distinctExprs = parseInfo
        .getDistinctFuncOverExprForClause(dest);

    if (distinctExprs != null && orderByExprs != null)
      throw new SemanticException(
          "distinct and order by can not exist in analysis functions at the same time!");

    Operator reduceSinkOperatorInfo = genAnalysisPlanReduceSinkOperator(qb,
        dest, input, partitionByExprs, orderByExprs, distinctExprs, numReducers);

    Operator analysisOperatorInfo = genAnalysisPlanAnalysisOperator(parseInfo,
        dest, reduceSinkOperatorInfo, analysisExprs);

    return analysisOperatorInfo;
  }

  private List<ASTNode> getPartitionByExprs(ASTNode functionOverExpr) {
    List<ASTNode> partitionByExprs = new ArrayList<ASTNode>();

    for (int i = 0; i < functionOverExpr.getChildCount(); i++) {
      if (functionOverExpr.getChild(i).getType() == HiveParser.TOK_PARTITIONBY) {
        ASTNode partitionByExpr = (ASTNode) functionOverExpr.getChild(i);

        for (int j = 0; j < partitionByExpr.getChildCount(); j++)
          partitionByExprs.add((ASTNode) partitionByExpr.getChild(j));

        break;
      }
    }

    return partitionByExprs;
  }

  private List<ASTNode> getOrderByExprs(ASTNode functionOverExpr) {
    for (int i = 0; i < functionOverExpr.getChildCount(); i++) {
      if (functionOverExpr.getChild(i).getType() == HiveParser.TOK_ORDERBY) {
        ASTNode orderByExpr = (ASTNode) functionOverExpr.getChild(i);
        List<ASTNode> orderByExprs = new ArrayList<ASTNode>();

        for (int j = 0; j < orderByExpr.getChildCount(); j++)
          orderByExprs.add((ASTNode) orderByExpr.getChild(j));

        return orderByExprs;
      }
    }

    return null;
  }

  static ArrayList<GenericUDAFEvaluator> getUDAFEvaluators(
      ArrayList<aggregationDesc> aggs) {
    ArrayList<GenericUDAFEvaluator> result = new ArrayList<GenericUDAFEvaluator>();
    for (int i = 0; i < aggs.size(); i++) {
      result.add(aggs.get(i).getGenericUDAFEvaluator());
    }
    return result;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MRMultiGroupBy(String dest, QB qb,
      Operator input) throws SemanticException {

    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = new LinkedHashMap<String, GenericUDAFEvaluator>();

    QBParseInfo parseInfo = qb.getParseInfo();

    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator1(parseInfo,
        dest, input, groupByDesc.Mode.HASH, genericUDAFEvaluators, true);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL,
        genericUDAFEvaluators);

    return groupByOperatorInfo2;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, input,
        (parseInfo.getDistinctFuncExprsForClause(dest).isEmpty() ? -1
            : Integer.MAX_VALUE), -1, false, true);

    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanGroupByOperator(
        parseInfo, dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIAL1,
        genericUDAFEvaluators);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL,
        genericUDAFEvaluators);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<ASTNode> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty())
      return false;

    if (!qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty())
      return false;

    return true;
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr1MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanMapGroupByOperator(
        qb, dest, inputOperatorInfo, groupByDesc.Mode.HASH,
        genericUDAFEvaluators);

    int numReducers = -1;

    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, groupByOperatorInfo, grpByExprs.size(), numReducers, true, false);

    return genGroupByPlanGroupByOperator1(parseInfo, dest,
        reduceSinkOperatorInfo, groupByDesc.Mode.MERGEPARTIAL,
        genericUDAFEvaluators, false);
  }

  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr2MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanMapGroupByOperator(
        qb, dest, inputOperatorInfo, groupByDesc.Mode.HASH,
        genericUDAFEvaluators);

    if (!optimizeMapAggrGroupBy(dest, qb)) {
      Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo,
          (parseInfo.getDistinctFuncExprsForClause(dest).isEmpty() ? -1
              : Integer.MAX_VALUE), -1, true, true);

      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo,
          dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIALS,
          genericUDAFEvaluators, false);

      int numReducers = -1;
      List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
      if (grpByExprs.isEmpty())
        numReducers = 1;

      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size(), numReducers);

      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL,
          genericUDAFEvaluators);
    } else {
      Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo, getGroupByForClause(parseInfo, dest)
              .size(), 1, true, false);

      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo, groupByDesc.Mode.FINAL, genericUDAFEvaluators);
    }
  }

  @SuppressWarnings("nls")
  private Operator genConversionOps(String dest, QB qb, Operator input)
      throws SemanticException {

    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);
    Table dest_tab = null;
    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {
      dest_tab = qb.getMetaData().getDestTableForAlias(dest);
      break;
    }
    case QBMetaData.DEST_PARTITION: {
      dest_tab = qb.getMetaData().getDestTableForAlias(dest);
      break;
    }
    default: {
      return input;
    }
    }

    return input;
  }

  private int getReducersNumber(int totalFiles, int maxReducers) {
    int numFiles = totalFiles / maxReducers;
    while (true) {
      if (totalFiles % numFiles == 0)
        return totalFiles / numFiles;
      numFiles++;
    }
  }

  private ArrayList<exprNodeDesc> getReduceParitionColsFromPartitionCols(
      Table tab, Operator input) {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    List<FieldSchema> tabCols = tab.getCols();
    String tabPartitionCol;

    if (!tab.getHasSubPartition()) {
      tabPartitionCol = tab.getTTable().getPriPartition().getParKey().getName();
    } else
      tabPartitionCol = tab.getTTable().getSubPartition().getParKey().getName();

    LOG.debug("The reduce partition column is: " + tabPartitionCol);
    ArrayList<exprNodeDesc> partitionCols = new ArrayList<exprNodeDesc>();

    int pos = 0;
    for (FieldSchema tabCol : tabCols) {
      if (tabPartitionCol.equals(tabCol.getName())) {
        LOG.debug("The reduce partition column is the " + pos + " column : "
            + tabCol.getName() + " in table: " + tab.getName());
        ColumnInfo colInfo = inputRR.getColumnInfos().get(pos);
        partitionCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo
            .getInternalName(), colInfo.getTabAlias(), colInfo
            .getIsPartitionCol()));
        LOG.debug("The reduce partition column is: " + " row : "
            + tabPartitionCol + " in table: " + tab.getName());

        break;
      }
      pos++;
    }

    return partitionCols;
  }

  private int estimateNumberOfInsertReducers(TableScanOperator input)
      throws IOException {
    long bytesPerReducer = conf
        .getLongVar(HiveConf.ConfVars.BYTESPERINSERTREDUCER);
    int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    TablePartition table = this.topToTable.get(input);
    long totalInputFileSize = getTotalInputFileSize(table);

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
        + maxReducers + " totalInputFileSize=" + totalInputFileSize);

    int reducers = (int) ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;
  }

  private long getTotalInputFileSize(TablePartition table) throws IOException {
    long r = 0;

    for (Path path : table.getPaths()) {
      try {
        LOG.info("Get length of path: " + path);
        FileSystem fs = path.getFileSystem(conf);
        ContentSummary cs = fs.getContentSummary(path);
        r += cs.getLength();
      } catch (IOException e) {
        LOG.info("Cannot get size of " + path + ". Safely ignored.");
      }
    }
    return r;
  }

  private Operator genReduceSinkPlanForPartitioner(Table tab, Operator input,
      ArrayList<exprNodeDesc> partitionCols, int numReducers)
      throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRR();

    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      valueCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsPartitionCol()));
      colExprMap.put(colInfo.getInternalName(),
          valueCols.get(valueCols.size() - 1));
    }

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++) {
      outputColumns.add(getColumnInternalName(i));
    }
    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(partitionCols, valueCols, outputColumns, false, -1,
            partitionCols, "+", numReducers),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    interim.setColumnExprMap(colExprMap);

    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
            Utilities.ReduceField.VALUE.toString(), "", false)), new RowSchema(
            out_rwsch.getColumnInfos()), interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for table: " + tab.getName()
        + " row schema: " + out_rwsch.toString());
    return output;
  }

  private boolean hasHashPartition(Table table) {
    if (table.getHasSubPartition()
        && table.getTTable().getSubPartition().getParType().equals("hash"))
      return true;
    if (!table.getHasSubPartition()
        && table.getTTable().getPriPartition().getParType().equals("hash"))
      return true;
    return false;
  }

  void testoprr(Operator input, String tag) {
    RowResolver inputRR = opParseCtx.get(input).getRR();
  }

  private Operator genLocalFileSinkPlan(String dest, QB qb, Operator input)
      throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBMetaData qbm = qb.getMetaData();
    Integer destType = qbm.getDestTypeForAlias(dest);
    Boolean isoverwrite = qbm.isDestOverwriteForAlias(dest);

    String queryTmpdir = null;
    Path destPath = null;
    tableDesc tblDescriptor = null;
    int currentTableId = 0;
    boolean isLocal = false;

    switch (destType.intValue()) {
    case QBMetaData.DEST_LOCAL_FILE:
      LOG.info("Insert into local file");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Insert into local file");
      isLocal = true;

    case QBMetaData.DEST_DFS_FILE: {
      LOG.info("Insert into distribute file");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Insert into distribute file");

      destPath = new Path(qbm.getDestFileForAlias(dest));
      String destStr = destPath.toString();

      if (isLocal) {
        queryTmpdir = ctx.getMRTmpFileURI();
      } else {
        try {
          Path qPath = FileUtils.makeQualified(destPath, conf);
          queryTmpdir = ctx.getExternalTmpFileURI(qPath.toUri());
        } catch (Exception e) {
          throw new SemanticException("Error creating temporary folder on: "
              + destPath, e);
        }
      }

      String cols = new String();
      String colTypes = new String();
      Vector<ColumnInfo> colInfos = inputRR.getColumnInfos();

      boolean first = true;

      for (ColumnInfo colInfo : colInfos) {
        String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

        if (nm[1] != null) {
          colInfo.setAlias(nm[1]);
        }

        if (!first) {
          cols = cols.concat(",");
          colTypes = colTypes.concat(":");
        }

        first = false;
        cols = cols.concat(colInfo.getInternalName());

        String tName = colInfo.getType().getTypeName();
        if (tName.equals(Constants.VOID_TYPE_NAME))
          colTypes = colTypes.concat(Constants.STRING_TYPE_NAME);
        else
          colTypes = colTypes.concat(tName);
      }

      if (!ctx.isMRTmpFileURI(destStr)) {
        this.idToTableNameMap.put(String.valueOf(this.destTableId), destStr);
        currentTableId = this.destTableId;
        this.destTableId++;
      }

      boolean isDfsDir = (destType.intValue() == QBMetaData.DEST_DFS_FILE);
      this.loadFileWork.add(new loadFileDesc(queryTmpdir, destStr, isDfsDir,
          cols, colTypes));

      tblDescriptor = PlanUtils.getDefaultTableDesc(
          Integer.toString(Utilities.ctrlaCode), cols, colTypes, false);

      if (!outputs.add(new WriteEntity(destStr, !isDfsDir))) {
        throw new SemanticException(
            ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(destStr));
      }
    }

      break;

    default:
      throw new SemanticException(
          "select without from cause can only be DEST_LOCAL_FILE or DEST_DFS_FILE");
    }

    inputRR = opParseCtx.get(input).getRR();
    Vector<ColumnInfo> vecCol = new Vector<ColumnInfo>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector) tblDescriptor
          .getDeserializer().getObjectInspector();
      List<? extends StructField> fields = rowObjectInspector
          .getAllStructFieldRefs();

      for (int i = 0; i < fields.size(); i++) {
        vecCol.add(new ColumnInfo(fields.get(i).getFieldName(), TypeInfoUtils
            .getTypeInfoFromObjectInspector(fields.get(i)
                .getFieldObjectInspector()), "", false));
      }
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }

    RowSchema fsRS = new RowSchema(vecCol);

    LocalFileSinkDesc fsDesc = new LocalFileSinkDesc(queryTmpdir,
        tblDescriptor, conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT),
        currentTableId);

    LOG.debug("file descriptor : " + fsDesc);

    Operator output = putOpInsertMap(
        OperatorFactory.getAndMakeChild(fsDesc, fsRS, input), inputRR);

    LOG.info("Created FileSink Plan for clause: " + dest + "dest_path: "
        + destPath + " row schema: " + inputRR.toString());

    if (qb.getIsQuery() && indexQueryInfo.isIndexMode) {
      indexQueryInfo.fieldList = getFieldListFromRR(inputRR, allColsList);
    }

    return output;
  }

  @SuppressWarnings("nls")
  private Operator genFileSinkPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBMetaData qbm = qb.getMetaData();
    Integer dest_type = qbm.getDestTypeForAlias(dest);
    Boolean isoverwrite = qbm.isDestOverwriteForAlias(dest);

    Table dest_tab = null;
    String queryTmpdir = null;
    Path dest_path = null;
    tableDesc table_desc = null;
    int currentTableId = 0;
    boolean isLocal = false;

    boolean multiFileSpray = false;
    int numFiles = 1;
    int totalFiles = 1;
    ArrayList<exprNodeDesc> partnCols = null;

    InsertPartDesc insertPartDesc = null;

    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {
      LOG.debug("Insert into table");
      
      dest_tab = qbm.getDestTableForAlias(dest);

      if (dest_tab.isPartitioned() && hasHashPartition(dest_tab)) {
        LOG.debug("Need use reduce task to insert into hashed table!");
        int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);

        int numLeafPartitions = 0;
        if (dest_tab.getHasSubPartition()) {
          numLeafPartitions = dest_tab.getTTable().getSubPartition()
              .getParSpacesSize();
        } else
          numLeafPartitions = dest_tab.getTTable().getPriPartition()
              .getParSpacesSize();

        int partPerReduce = conf
            .getIntVar(HiveConf.ConfVars.FILESPERINSERTREDUCER);
        maxReducers = numLeafPartitions / partPerReduce;
        LOG.debug("Reduce task number is: " + maxReducers);
        partnCols = getReduceParitionColsFromPartitionCols(dest_tab, input);

        input = genReduceSinkPlanForPartitioner(dest_tab, input, partnCols,
            maxReducers);
      }

      dest_path = dest_tab.getPath();
      queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
      table_desc = Utilities.getTableDesc(dest_tab);
      table_desc.setDBName(dest_tab.getDbName());

      this.idToTableNameMap.put(String.valueOf(this.destTableId),
          dest_tab.getName());
      currentTableId = this.destTableId;
      this.destTableId++;

      this.loadTableWork.add(new loadTableDesc(queryTmpdir, ctx
          .getExternalTmpFileURI(dest_path.toUri()), table_desc,
          new HashMap<String, String>(), isoverwrite));

      if (!outputs.add(new WriteEntity(dest_tab))) {
        throw new SemanticException(
            ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(dest_tab.getName()));

      }

      break;
    }

    case QBMetaData.DEST_PARTITION:

      LOG.info("Insert into partition");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Insert into partition");

      dest_tab = qbm.getDestTableForAlias(dest);
      table_desc = Utilities.getTableDesc(dest_tab);
      table_desc.setDBName(dest_tab.getDbName());

      this.idToTableNameMap.put(String.valueOf(this.destTableId),
          dest_tab.getName());
      currentTableId = this.destTableId;
      this.destTableId++;

      insertPartDesc = qbm.getInsertPartDesc(dest);

      List<String> partList = insertPartDesc.getPartList();
      List<String> subPartList = insertPartDesc.getSubPartList();
      PartRefType pt = insertPartDesc.getPartType();
      Path destPath = null;
      String queryTmpDir = null;

      switch (pt) {
      case PRI:
        for (String partName : partList) {
          destPath = new Path(dest_tab.getPath(), partName);
          queryTmpDir = ctx.getExternalTmpFileURI(destPath.toUri());

          this.loadTableWork.add(new loadTableDesc(queryTmpDir, ctx
              .getExternalTmpFileURI(destPath.toUri()), table_desc, partName,
              null, pt, isoverwrite));
          queryTmpdir = queryTmpDir;
        }

        break;

      case SUB:
        throw new SemanticException("you must assiagn a pri-partition");

      case COMP:

        for (String partName : partList) {
          destPath = new Path(dest_tab.getPath(), partName);

          for (String subPartName : subPartList) {
            Path subPath = new Path(destPath, subPartName);
            queryTmpDir = ctx.getExternalTmpFileURI(subPath.toUri());
            this.loadTableWork.add(new loadTableDesc(queryTmpDir, ctx
                .getExternalTmpFileURI(destPath.toUri()), table_desc, partName,
                subPartName, pt, isoverwrite));
            queryTmpdir = queryTmpDir;
          }
        }
        break;
      default:
        break;

      }

      if (!outputs.add(new WriteEntity(dest_tab))) {
        throw new SemanticException(
            ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(dest_tab.getName()));
      }

      break;

    case QBMetaData.DEST_LOCAL_FILE:
      LOG.info("Insert into local file");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Insert into local file");
      isLocal = true;
    case QBMetaData.DEST_DFS_FILE: {
      LOG.info("Insert into distribute file");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Insert into distribute file!" + dest);
      dest_path = new Path(qbm.getDestFileForAlias(dest));
      String destStr = dest_path.toString();

      if (isLocal) {
        queryTmpdir = ctx.getMRTmpFileURI();
      } else {

        try {
          Path qPath = FileUtils.makeQualified(dest_path, conf);
          queryTmpdir = ctx.getExternalTmpFileURI(qPath.toUri());
        } catch (Exception e) {
          throw new SemanticException("Error creating temporary folder on: "
              + dest_path, e);
        }
      }
      String cols = new String();
      String colTypes = new String();
      Vector<ColumnInfo> colInfos = inputRR.getColumnInfos();

      List<FieldSchema> field_schemas = null;
      createTableDesc tblDesc = qb.getTableDesc();
      if (tblDesc != null)
        field_schemas = new ArrayList<FieldSchema>();

      boolean first = true;
      for (ColumnInfo colInfo : colInfos) {
        String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

        if (nm[1] != null) {
          colInfo.setAlias(nm[1]);
        }

        if (field_schemas != null) {
          FieldSchema col = new FieldSchema();
          if (nm[1] != null) {
            col.setName(colInfo.getAlias());
          } else {
            col.setName(colInfo.getInternalName());
          }
          col.setType(colInfo.getType().getTypeName());
          field_schemas.add(col);
        }

        if (!first) {
          cols = cols.concat(",");
          colTypes = colTypes.concat(":");
        }

        first = false;
        cols = cols.concat(colInfo.getInternalName());

        String tName = colInfo.getType().getTypeName();
        if (tName.equals(Constants.VOID_TYPE_NAME))
          colTypes = colTypes.concat(Constants.STRING_TYPE_NAME);
        else
          colTypes = colTypes.concat(tName);
      }

      if (qb.isCTAS()) {
        int num = 0;
        Iterator<FieldSchema> iterCols = field_schemas.iterator();
        List<String> colNames = new ArrayList<String>();
        while (iterCols.hasNext()) {
          String colName = iterCols.next().getName();
          Iterator<String> iter = colNames.iterator();
          while (iter.hasNext()) {
            String oldColName = iter.next();
            if (colName.equalsIgnoreCase(oldColName)) {
              colName = colName + num;
              num++;
            }
          }
          colNames.add(colName);
        }

        for (int i = 0; i < colNames.size(); i++) {
          if (!colNames.get(i).equalsIgnoreCase(field_schemas.get(i).getName())) {
            field_schemas.get(i).setName(colNames.get(i));
          }
        }
      }

      if (tblDesc != null)
        tblDesc.setCols(field_schemas);

      if (!ctx.isMRTmpFileURI(destStr)) {
        this.idToTableNameMap.put(String.valueOf(this.destTableId), destStr);
        currentTableId = this.destTableId;
        this.destTableId++;
      }

      if (tblDesc == null) {
        table_desc = PlanUtils.getDefaultTableDesc(
            Integer.toString(Utilities.ctrlaCode), cols, colTypes, false);
      } else {
        table_desc = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
      }

      boolean insertTmpDir = qbm.getDestForTmpdir(dest);
      if (insertTmpDir && multiHdfsInfo.isMultiHdfsEnable()) {
        try {
          String mydbname = table_desc.getDBName();
          if (mydbname == null) {
            mydbname = SessionState.get().getDbName().toLowerCase();
          }
          ArrayList<String> dirList = new ArrayList<String>();
          boolean changeDir = multiHdfsInfo.processTmpDir(queryTmpdir, destStr,
              mydbname, qb.isCTAS(), dirList);
          if (changeDir) {
            destStr = dirList.get(0);
            ctx.setResDir(new Path(destStr));
            queryTmpdir = dirList.get(1);
            ctx.addScratchDir(new Path(queryTmpdir).getParent());
            LOG.debug("update deststr for " + dest);
            qbm.getNameToDestFile().put(dest, destStr);
          }
        } catch (MetaException e) {
          throw (new SemanticException(e.getMessage()));
        }
      }

      boolean isDfsDir = (dest_type.intValue() == QBMetaData.DEST_DFS_FILE);
      this.loadFileWork.add(new loadFileDesc(queryTmpdir, destStr, isDfsDir,
          cols, colTypes));

      if (!outputs.add(new WriteEntity(destStr, !isDfsDir))) {
        throw new SemanticException(
            ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(destStr));
      }

      break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    input = genConversionSelectOperator(dest, qb, input, table_desc);
    inputRR = opParseCtx.get(input).getRR();

    Vector<ColumnInfo> vecCol = new Vector<ColumnInfo>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector) table_desc
          .getDeserializer().getObjectInspector();
      List<? extends StructField> fields = rowObjectInspector
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++)
        vecCol.add(new ColumnInfo(fields.get(i).getFieldName(), TypeInfoUtils
            .getTypeInfoFromObjectInspector(fields.get(i)
                .getFieldObjectInspector()), "", false));
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }

    RowSchema fsRS = new RowSchema(vecCol);

    fileSinkDesc fsDesc;
    if (dest_tab != null && dest_tab.isPartitioned()) {
      ArrayList<String> partTypes = new ArrayList<String>();
      ArrayList<String> partTypeInfos = new ArrayList<String>();
      ArrayList<exprNodeDesc> partKeys = new ArrayList<exprNodeDesc>();
      ArrayList<PartSpaceSpec> partSpaces = new ArrayList<PartSpaceSpec>();
      ArrayList<RangePartitionExprTree> exprTrees = new ArrayList<RangePartitionExprTree>();

      genPartitionInfo(dest_tab.getTTable(), dest_tab.getTTable()
          .getPriPartition(), inputRR, partTypes, partTypeInfos, partKeys,
          partSpaces, exprTrees);
      if (dest_tab.getTTable().getSubPartition() != null) {
        genPartitionInfo(dest_tab.getTTable(), dest_tab.getTTable()
            .getSubPartition(), inputRR, partTypes, partTypeInfos, partKeys,
            partSpaces, exprTrees);
      }

      fsDesc = new partitionSinkDesc(queryTmpdir, table_desc,
          conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT), currentTableId,
          partTypes, partTypeInfos, partKeys, partSpaces, exprTrees,
          insertPartDesc);
    } else {
      fsDesc = new fileSinkDesc(queryTmpdir, table_desc,
          conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT), currentTableId);
    }

    Operator output = putOpInsertMap(
        OperatorFactory.getAndMakeChild(fsDesc, fsRS, input), inputRR);

    LOG.info("Created FileSink Plan for clause: " + dest + "dest_path: "
        + dest_path + " row schema: " + inputRR.toString());

    if (qb.getIsQuery() && indexQueryInfo.isIndexMode) {
      indexQueryInfo.fieldList = getFieldListFromRR(inputRR, allColsList);
    }

    return output;
  }

  private String getFieldListFromRR(RowResolver rr, List<FieldSchema> fields) {
    StringBuffer sb = new StringBuffer();

    int count = 0;
    for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : rr.rslvMap()
        .entrySet()) {
      HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>) e
          .getValue();
      if (f_map != null) {
        for (Map.Entry<String, ColumnInfo> entry : f_map.entrySet()) {
          if (count == 0) {
            sb.append(getFieldIndxByName((String) entry.getKey(), fields));
          } else {
            sb.append("," + getFieldIndxByName((String) entry.getKey(), fields));
          }

          count++;
        }
      }
    }
    return sb.toString();
  }

  private int getFieldIdxInSchema(
      org.apache.hadoop.hive.metastore.api.Table table, String fieldName) {
    int i = 0;
    Iterator<FieldSchema> iter = table.getSd().getColsIterator();
    while (iter.hasNext()) {
      FieldSchema fs = iter.next();
      if (fs.getName().equalsIgnoreCase(fieldName))
        return i;
      i++;
    }
    return -1;
  }

  private void genPartitionInfo(
      org.apache.hadoop.hive.metastore.api.Table table,
      org.apache.hadoop.hive.metastore.api.Partition partition, RowResolver rr,
      ArrayList<String> partTypes, ArrayList<String> partTypeInfos,
      ArrayList<exprNodeDesc> partKeys, ArrayList<PartSpaceSpec> partSpaces,
      ArrayList<RangePartitionExprTree> exprTrees) throws SemanticException {
    partTypes.add(partition.getParType());
    partSpaces.add(PartSpaceSpec.convertToPartSpaceSpec(partition
        .getParSpaces()));

    String partKeyName = partition.getParKey().getName();

    int idx = getFieldIdxInSchema(table, partKeyName);
    if (idx < 0)
      throw new SemanticException("Can't find field " + partKeyName
          + " in table " + table.getTableName() + ".");

    String field = getColumnInternalName(idx);

    TypeInfo partKeyType = TypeInfoFactory.getPrimitiveTypeInfo(partition
        .getParKey().getType());
    exprNodeDesc partKeyDesc = new exprNodeColumnDesc(partKeyType, field, "",
        true);
    partKeys.add(partKeyDesc);
    partTypeInfos.add(partition.getParKey().getType());

    if (partition.getParType().equalsIgnoreCase("RANGE")) {
      exprTrees.add(getRangePartitionExprTree(partKeyType.getTypeName(),
          partKeyDesc, partition.getParSpaces()));
    } else {
      exprTrees.add(null);
    }
  }

  private RangePartitionExprTree getRangePartitionExprTree(
      String partKeyTypeName, exprNodeDesc partKey,
      Map<String, List<String>> partSpace) {
    TypeInfo partKeyType = TypeInfoFactory
        .getPrimitiveTypeInfo(partKeyTypeName);

    ObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) partKeyType)
            .getPrimitiveCategory());

    ArrayList<exprNodeDesc> exprTree = new ArrayList<exprNodeDesc>();
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      String partName = entry.getKey();
      List<String> partValues = entry.getValue();

      if (partName.equalsIgnoreCase("default")) {
        exprTree.add(null);
      } else {
        ObjectInspectorConverters.Converter converter = ObjectInspectorConverters
            .getConverter(stringOI, valueOI);
        Object pv = converter.convert(partValues.get(0));
        pv = ((PrimitiveObjectInspector) valueOI).getPrimitiveJavaObject(pv);

        exprNodeDesc partValueDesc = new exprNodeConstantDesc(partKeyType, pv);
        exprNodeDesc compareDesc = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("comparison", partValueDesc, partKey);

        exprTree.add(compareDesc);
      }
    }

    return new RangePartitionExprTree(exprTree);
  }

  Operator genConversionSelectOperator(String dest, QB qb, Operator input,
      tableDesc table_desc) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass()
          .newInstance();
      deserializer.initialize(conf, table_desc.getProperties());
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    Vector<ColumnInfo> rowFields = opParseCtx.get(input).getRR()
        .getColumnInfos();

    boolean insertColumnsExist = qb.getMetaData().getInsertColumnsToDestTable()
        .containsKey(dest);
    if (!insertColumnsExist && tableFields.size() != rowFields.size()) {

      String reason = "Table " + dest + " has " + tableFields.size()
          + " columns but query has " + rowFields.size() + " columns.";
      throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
          qb.getParseInfo().getDestForClause(dest), reason));
    }

    boolean converted = false;
    ArrayList<exprNodeDesc> expressions = new ArrayList<exprNodeDesc>();
    boolean isMetaDataSerDe = table_desc.getDeserializerClass().equals(
        MetadataTypedColumnsetSerDe.class);
    boolean isLazySimpleSerDe = table_desc.getDeserializerClass().equals(
        LazySimpleSerDe.class);
    HashMap<String, Integer> columnname2pos = new HashMap<String, Integer>();
    if (insertColumnsExist) {
      converted = true;
      ArrayList<String> columns = qb.getMetaData()
          .getInsertColumnsToDestTable().get(dest);
      if (columns.size() != rowFields.size()) {
        String reason = "Table " + dest + " has " + columns.size()
            + " columns but query select has " + rowFields.size() + " columns.";
        throw new SemanticException(
            ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(qb.getParseInfo()
                .getDestForClause(dest), reason));
      }

      for (int i = 0; i < columns.size(); i++) {
        columnname2pos.put(columns.get(i), i);
      }
    }
    if (!isMetaDataSerDe || insertColumnsExist) {
      int columnNumber = tableFields.size();
      for (int i = 0; i < columnNumber; i++) {
        ObjectInspector tableFieldOI = tableFields.get(i)
            .getFieldObjectInspector();
        TypeInfo tableFieldTypeInfo = TypeInfoUtils
            .getTypeInfoFromObjectInspector(tableFieldOI);
        TypeInfo rowFieldTypeInfo;
        exprNodeDesc column;
        if (insertColumnsExist) {
          if (columnname2pos.containsKey(tableFields.get(i).getFieldName())) {
            ColumnInfo cinfo = rowFields.get(columnname2pos.get(tableFields
                .get(i).getFieldName()));
            rowFieldTypeInfo = cinfo.getType();
            column = new exprNodeColumnDesc(rowFieldTypeInfo,
                cinfo.getInternalName(), "", false);
          } else {
            rowFieldTypeInfo = tableFieldTypeInfo;
            column = new exprNodeNullDesc();
          }
        } else {
          rowFieldTypeInfo = rowFields.get(i).getType();
          column = new exprNodeColumnDesc(rowFieldTypeInfo, rowFields.get(i)
              .getInternalName(), "", false);
        }
        if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)
            && !(isLazySimpleSerDe
                && tableFieldTypeInfo.getCategory().equals(Category.PRIMITIVE) && tableFieldTypeInfo
                  .equals(TypeInfoFactory.stringTypeInfo))) {
          converted = true;
          if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
            column = null;
          } else {
            column = TypeCheckProcFactory.DefaultExprProcessor
                .getFuncExprNodeDesc(tableFieldTypeInfo.getTypeName(), column);
          }
          if (column == null) {
            String reason = "Cannot convert column " + i + " from "
                + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
            throw new SemanticException(
                ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(qb.getParseInfo()
                    .getDestForClause(dest), reason));
          }
        }
        expressions.add(column);
      }
    }

    Operator output = input;
    if (converted) {
      RowResolver rowResolver = new RowResolver();
      ArrayList<String> colName = new ArrayList<String>();
      Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

      for (int i = 0; i < expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colName.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      output = putOpInsertMap(OperatorFactory.getAndMakeChild(new selectDesc(
          expressions, colName), new RowSchema(rowResolver.getColumnInfos()),
          input), rowResolver);
      output.setColumnExprMap(colExprMap);
    }

    return output;
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, QB qb, Operator input, int limit)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    Operator limitMap = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new limitDesc(limit), new RowSchema(inputRR.getColumnInfos()), input),
        inputRR);

    LOG.debug("Created LimitOperator Plan for clause: " + dest
        + " row schema: " + inputRR.toString());

    return limitMap;
  }

  private Operator genUDTFPlan(GenericUDTF genericUDTF,
      String outputTableAlias, ArrayList<String> colAliases, QB qb,
      Operator input) throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();
    if (!qbp.getDestToGroupBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
    }
    if (!qbp.getDestToDistributeBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
    }
    if (!qbp.getDestToSortBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
    }
    if (!qbp.getDestToClusterBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
    }
    if (!qbp.getAliasToLateralViews().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
    }

    LOG.debug("Table alias: " + outputTableAlias + " Col aliases: "
        + colAliases);

    RowResolver selectRR = opParseCtx.get(input).getRR();
    Vector<ColumnInfo> inputCols = selectRR.getColumnInfos();

    ArrayList<String> colNames = new ArrayList<String>();
    ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
    for (int i = 0; i < inputCols.size(); i++) {
      colNames.add(inputCols.get(i).getInternalName());
      colOIs[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(inputCols.get(i)
              .getType());
    }
    StructObjectInspector outputOI = genericUDTF.initialize(colOIs);

    int numUdtfCols = outputOI.getAllStructFieldRefs().size();
    int numSuppliedAliases = colAliases.size();
    if (numUdtfCols != numSuppliedAliases) {
      throw new SemanticException(
          ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg("expected " + numUdtfCols
              + " aliases " + "but got " + numSuppliedAliases));
    }

    ArrayList<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

    Iterator<String> colAliasesIter = colAliases.iterator();
    for (StructField sf : outputOI.getAllStructFieldRefs()) {

      String colAlias = colAliasesIter.next();
      assert (colAlias != null);

      ColumnInfo col = new ColumnInfo(sf.getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(sf
              .getFieldObjectInspector()), outputTableAlias, false);
      udtfCols.add(col);
    }

    RowResolver out_rwsch = new RowResolver();
    for (int i = 0; i < udtfCols.size(); i++) {
      out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
    }

    Operator<?> udtf = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new udtfDesc(genericUDTF), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);
    return udtf;
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input,
      int limit, boolean extraMRStep) throws SemanticException {
    Operator curr = genLimitPlan(dest, qb, input, limit);

    if (!extraMRStep)
      return curr;

    curr = genReduceSinkPlan(dest, qb, curr, 1);
    return genLimitPlan(dest, qb, curr, limit);
  }

  @SuppressWarnings("nls")
  private Operator genHalfSortPlan(String dest, QB qb, Operator input, int limit)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    ASTNode orderByExprs = qb.getParseInfo().getOrderByForClause(dest);

    ArrayList<exprNodeDesc> sortCols = new ArrayList<exprNodeDesc>();
    StringBuilder order = new StringBuilder();
    if (orderByExprs != null) {
      int ccount = orderByExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) orderByExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
        } else {
          order.append("+");
        }
        exprNodeDesc exprNode = genExprNodeDesc(cl, inputRR, qb);
        sortCols.add(exprNode);
      }
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        PlanUtils.getHalfSortLimitDesc(sortCols, order.toString(), limit),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    LOG.debug("Created HalfSort Plan for clause: " + dest + " row schema: "
        + inputRR.toString());
    return output;

  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(String dest, QB qb, Operator input,
      int numReducers) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    ArrayList<exprNodeDesc> partitionCols = new ArrayList<exprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        LOG.debug("distribute expr : " + cl.toStringTree());
        partitionCols.add(genExprNodeDesc(cl, inputRR, qb));
      }
    }

    ASTNode sortExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getSortByForClause(dest);
    }

    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getOrderByForClause(dest);
      if (sortExprs != null) {
        assert numReducers == 1;
        Integer limit = qb.getParseInfo().getDestLimit(dest);
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
            "strict")
            && limit == null)
          throw new SemanticException(
              ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg(sortExprs));
      }
    }

    ArrayList<exprNodeDesc> sortCols = new ArrayList<exprNodeDesc>();
    StringBuilder order = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
        } else {
          order.append("+");
        }
        exprNodeDesc exprNode = genExprNodeDesc(cl, inputRR, qb);
        sortCols.add(exprNode);
      }
    }

    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      exprNodeColumnDesc eee = new exprNodeColumnDesc(colInfo.getType(),
          colInfo.getInternalName(), colInfo.getTabAlias(),
          colInfo.getIsPartitionCol());
      LOG.debug("colInfo in reducesink:  " + colInfo.getType().getTypeName());
      if (colInfo.getType().getTypeName().equalsIgnoreCase("VOID")) {
        valueCols.add(new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo,
            null));
      } else {
        valueCols.add(eee);
      }
      colExprMap.put(colInfo.getInternalName(),
          valueCols.get(valueCols.size() - 1));
    }

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++)
      outputColumns.add(getColumnInternalName(i));
    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(sortCols, valueCols, outputColumns, false, -1,
            partitionCols, order.toString(), numReducers), new RowSchema(
        inputRR.getColumnInfos()), input), inputRR);
    interim.setColumnExprMap(colExprMap);

    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
            Utilities.ReduceField.VALUE.toString(), "", false)), new RowSchema(
            out_rwsch.getColumnInfos()), interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for clause: " + dest + " row schema: "
        + out_rwsch.toString());
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left,
      Operator[] right, HashSet<Integer> omitOpts, QB qb)
      throws SemanticException {
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Operator<?>[] rightOps = new Operator[right.length];
    int outputPos = 0;

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    HashMap<Byte, List<exprNodeDesc>> exprMap = new HashMap<Byte, List<exprNodeDesc>>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();
    HashMap<Byte, List<exprNodeDesc>> filterMap = new HashMap<Byte, List<exprNodeDesc>>();

    for (int pos = 0; pos < right.length; ++pos) {

      Operator input = right[pos];
      if (input == null)
        input = left;

      ArrayList<exprNodeDesc> keyDesc = new ArrayList<exprNodeDesc>();
      ArrayList<exprNodeDesc> filterDesc = new ArrayList<exprNodeDesc>();
      Byte tag = Byte.valueOf((byte) (((reduceSinkDesc) (input.getConf()))
          .getTag()));

      if (omitOpts == null || !omitOpts.contains(pos)) {
        RowResolver inputRS = opParseCtx.get(input).getRR();
        Iterator<String> keysIter = inputRS.getTableNames().iterator();
        Set<String> aliases = posToAliasMap.get(pos);
        if (aliases == null) {
          aliases = new HashSet<String>();
          posToAliasMap.put(pos, aliases);
        }
        while (keysIter.hasNext()) {
          String key = keysIter.next();
          aliases.add(key);
          HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
          Iterator<String> fNamesIter = map.keySet().iterator();
          while (fNamesIter.hasNext()) {
            String field = fNamesIter.next();
            ColumnInfo valueInfo = inputRS.get(key, field);
            keyDesc.add(new exprNodeColumnDesc(valueInfo.getType(), valueInfo
                .getInternalName(), valueInfo.getTabAlias(), valueInfo
                .getIsPartitionCol()));

            if (outputRS.get(key, field) == null) {
              String colName = getColumnInternalName(outputPos);
              outputPos++;
              outputColumnNames.add(colName);
              colExprMap.put(colName, keyDesc.get(keyDesc.size() - 1));
              outputRS.put(key, field,
                  new ColumnInfo(colName, valueInfo.getType(), key, false));
              reversedExprs.put(colName, tag);
            }
          }
        }
        for (ASTNode cond : join.getFilters().get(tag)) {
          filterDesc.add(genExprNodeDesc(cond, inputRS, qb));
        }
      }

      exprMap.put(tag, keyDesc);
      filterMap.put(tag, filterDesc);
      rightOps[pos] = input;
    }

    org.apache.hadoop.hive.ql.plan.joinCond[] joinCondns = new org.apache.hadoop.hive.ql.plan.joinCond[join
        .getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      joinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new org.apache.hadoop.hive.ql.plan.joinCond(condn);
    }

    joinDesc desc = new joinDesc(exprMap, outputColumnNames,
        join.getNoOuterJoin(), joinCondns, filterMap);
    desc.setReversedExprs(reversedExprs);
    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(desc,
        new RowSchema(outputRS.getColumnInfos()), rightOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);
    return putOpInsertMap(joinOp, outputRS);
  }

  @SuppressWarnings("nls")
  private Operator genJoinReduceSinkChild(QB qb, QBJoinTree joinTree,
      Operator child, String srcName, int pos) throws SemanticException {
    if (child == null)
      LOG.debug("child is null!");
    LOG.debug("child : " + child.getClass().getName());
    RowResolver inputRS = opParseCtx.get(child).getRR();
    LOG.debug("inputRS : " + inputRS);
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumns = new ArrayList<String>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    Vector<ASTNode> exprs = joinTree.getExpressions().get(pos);
    for (int i = 0; i < exprs.size(); i++) {
      ASTNode expr = exprs.get(i);
      reduceKeys.add(genExprNodeDesc(expr, inputRS, qb));
    }

    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    while (tblNamesIter.hasNext()) {
      String src = tblNamesIter.next();
      HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();
        exprNodeColumnDesc inputExpr = new exprNodeColumnDesc(
            valueInfo.getType(), valueInfo.getInternalName(),
            valueInfo.getTabAlias(), valueInfo.getIsPartitionCol());
        reduceValues.add(inputExpr);
        if (outputRS.get(src, field) == null) {
          String col = getColumnInternalName(reduceValues.size() - 1);
          outputColumns.add(col);
          ColumnInfo newColInfo = new ColumnInfo(
              Utilities.ReduceField.VALUE.toString() + "." + col,
              valueInfo.getType(), src, false);
          colExprMap.put(newColInfo.getInternalName(), inputExpr);
          outputRS.put(src, field, newColInfo);
        }
      }
    }

    int numReds = -1;

    if (reduceKeys.size() == 0) {
      numReds = 1;

      if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
          "strict"))
        throw new SemanticException(ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumns, false, joinTree.getNextTag(),
            reduceKeys.size(), numReds),
            new RowSchema(outputRS.getColumnInfos()), child), outputRS);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = null;
    if (leftChild != null) {
      Operator joinOp = genJoinOperator(qb, leftChild, map);
      ArrayList<ASTNode> filter = joinTree.getFiltersForPushing().get(0);
      for (ASTNode cond : filter)
        joinOp = genFilterPlan(qb, cond, joinOp);

      joinSrcOp = genJoinReduceSinkChild(qb, joinTree, joinOp, null, 0);
    }

    Operator[] srcOps = new Operator[joinTree.getBaseSrc().length];

    HashSet<Integer> omitOpts = null;
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        LOG.debug("joinBaseSrc src : " + src);
        Operator srcOp = map.get(src);
        if (srcOp == null)
          LOG.debug("srcOP is null!");

        ArrayList<ASTNode> fields = joinTree.getRHSSemijoinColumns(src);
        if (fields != null) {
          if (omitOpts == null) {
            omitOpts = new HashSet<Integer>();
          }
          omitOpts.add(pos);

          srcOp = insertSelectForSemijoin(fields, srcOp, qb);

          srcOp = genMapGroupByForSemijoin(qb, fields, srcOp,
              groupByDesc.Mode.HASH);
        }

        srcOps[pos] = genJoinReduceSinkChild(qb, joinTree, srcOp, src, pos);
        pos++;
      } else {
        assert pos == 0;
        srcOps[pos++] = null;
      }
    }

    genJoinOperatorTypeCheck(joinSrcOp, srcOps);

    JoinOperator joinOp = (JoinOperator) genJoinOperatorChildren(joinTree,
        joinSrcOp, srcOps, omitOpts, qb);
    joinContext.put(joinOp, joinTree);
    return joinOp;
  }

  private Operator insertSelectForSemijoin(ArrayList<ASTNode> fields,
      Operator input, QB qb) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    ArrayList<exprNodeDesc> colList = new ArrayList<exprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();

    for (ASTNode field : fields) {
      exprNodeColumnDesc exprNode = (exprNodeColumnDesc) genExprNodeDesc(field,
          inputRR, qb);
      colList.add(exprNode);
      columnNames.add(exprNode.getColumn());
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(colList, columnNames, false),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    output.setColumnExprMap(input.getColumnExprMap());
    return output;
  }

  private Operator genMapGroupByForSemijoin(QB qb, ArrayList<ASTNode> fields,
      Operator inputOperatorInfo, groupByDesc.Mode mode)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    QBParseInfo parseInfo = qb.getParseInfo();

    groupByOutputRowResolver.setIsExprResolver(true);

    for (int i = 0; i < fields.size(); ++i) {
      ASTNode colName = fields.get(i);
      exprNodeDesc grpByExprNode = genExprNodeDesc(colName,
          groupByInputRowResolver, qb);
      groupByKeys.add(grpByExprNode);

      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo colInfo2 = new ColumnInfo(field, grpByExprNode.getTypeInfo(),
          "", false);
      groupByOutputRowResolver.putExpression(colName, colInfo2);

      colExprMap.put(field, grpByExprNode);
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);

    op.setColumnExprMap(colExprMap);
    return op;
  }

  private Operator genReduceSinkForSemijoin(QB qb, ArrayList<ASTNode> fields,
      Operator inputOperatorInfo) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    List<String> outputColumnNames = new ArrayList<String>();

    reduceSinkOutputRowResolver.setIsExprResolver(true);

    for (int i = 0; i < fields.size(); ++i) {
      ASTNode colName = fields.get(i);
      exprNodeDesc inputExpr = genExprNodeDesc(colName,
          reduceSinkInputRowResolver, qb);

      reduceKeys.add(inputExpr);

      if (reduceSinkOutputRowResolver.get("", colName.toStringTree()) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo1 = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), null, false);
        reduceSinkOutputRowResolver.putExpression(colName, colInfo1);
        colExprMap.put(colInfo1.getInternalName(), inputExpr);
      } else {
        throw new SemanticException(ErrorMsg.DUPLICATE_GROUPBY_KEY.getMsg());
      }
    }

    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    int numPartitionFields = fields.size();

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, numPartitionFields, -1),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
            inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);

    return rsOp;
  }

  private void genJoinOperatorTypeCheck(Operator left, Operator[] right)
      throws SemanticException {
    ArrayList<ArrayList<exprNodeDesc>> keys = new ArrayList<ArrayList<exprNodeDesc>>();
    int keyLength = 0;
    for (int i = 0; i < right.length; i++) {
      Operator oi = (i == 0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();
      if (i == 0) {
        keyLength = now.getKeyCols().size();
      } else {
        assert (keyLength == now.getKeyCols().size());
      }
      keys.add(now.getKeyCols());
    }
    for (int k = 0; k < keyLength; k++) {
      TypeInfo commonType = keys.get(0).get(k).getTypeInfo();
      for (int i = 1; i < right.length; i++) {
        TypeInfo a = commonType;
        TypeInfo b = keys.get(i).get(k).getTypeInfo();
        commonType = FunctionRegistry.getCommonClassForComparison(a, b);
        if (commonType == null) {
          throw new SemanticException(
              "Cannot do equality join on different types: " + a.getTypeName()
                  + " and " + b.getTypeName());
        }
      }
      for (int i = 0; i < right.length; i++) {
        if (!commonType.equals(keys.get(i).get(k).getTypeInfo())) {
          keys.get(i).set(
              k,
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
                  commonType.getTypeName(), keys.get(i).get(k)));
        }
      }
    }
    for (int i = 0; i < right.length; i++) {
      Operator oi = (i == 0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();

      now.setKeySerializeInfo(PlanUtils.getReduceKeyTableDesc(
          PlanUtils.getFieldSchemasFromColumnList(now.getKeyCols(), "joinkey"),
          now.getOrder()));
    }
  }

  private Operator genJoinPlan(QB qb, HashMap<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    Operator joinOp = genJoinOperator(qb, joinTree, map);
    return joinOp;
  }

  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    if (joinTree.getJoinSrc() != null)
      pushJoinFilters(qb, joinTree.getJoinSrc(), map);

    ArrayList<ArrayList<ASTNode>> filters = joinTree.getFiltersForPushing();
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        ArrayList<ASTNode> filter = filters.get(pos);
        for (ASTNode cond : filter)
          srcOp = genFilterPlan(qb, cond, srcOp);
        map.put(src, srcOp);
      }
      pos++;
    }
  }

  private List<String> getMapSideJoinTables(QB qb) throws SemanticException {
    List<String> cols = new ArrayList<String>();
    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (hint.getChildCount() > 0
          && ((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_MAPJOIN) {
        ASTNode hintTblNames = (ASTNode) hint.getChild(1);
        int numCh = hintTblNames.getChildCount();
        for (int tblPos = 0; tblPos < numCh; tblPos++) {
          String tblName = getFullAlias(qb,
              (ASTNode) (hintTblNames.getChild(tblPos)));
          if (tblName == null) {
            throw new SemanticException(
                "hint error in hint position : "
                    + tblPos
                    + " ,can't find table :"
                    + ((ASTNode) (hintTblNames.getChild(tblPos).getChild(0)))
                        .getText());
          }
          if (cols == null)
            cols = new ArrayList<String>();
          if (!cols.contains(tblName))
            cols.add(tblName);
        }
      }
    }

    return cols;
  }

  private QBJoinTree genUniqueJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinTree.setNoOuterJoin(false);

    joinTree.setExpressions(new Vector<Vector<ASTNode>>());
    joinTree.setFiltersForPushing(new ArrayList<ArrayList<ASTNode>>());

    Vector<String> rightAliases = new Vector<String>();
    Vector<String> leftAliases = new Vector<String>();
    Vector<String> baseSrc = new Vector<String>();
    Vector<Boolean> preserved = new Vector<Boolean>();

    boolean lastPreserved = false;
    int cols = -1;

    for (int i = 0; i < joinParseTree.getChildCount(); i++) {
      ASTNode child = (ASTNode) joinParseTree.getChild(i);

      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABREF:

        String table_name = unescapeIdentifier(child.getChild(0).getText())
            .toLowerCase();
        if (this.hasWith && child.getChildCount() == 1
            && this.withQueries.contains(table_name))
          throw new SemanticException(
              "Subqueries are not supported in UNIQUEJOIN");
        String alias = child.getChildCount() == 1 ? table_name
            : unescapeIdentifier(child.getChild(child.getChildCount() - 1)
                .getText().toLowerCase());

        if (i == 0) {
          leftAliases.add(alias);
          joinTree.setLeftAlias(alias);
        } else {
          rightAliases.add(alias);
        }
        baseSrc.add(alias);

        preserved.add(lastPreserved);
        lastPreserved = false;
        break;

      case HiveParser.TOK_EXPLIST:
        if (cols == -1 && child.getChildCount() != 0) {
          cols = child.getChildCount();
        } else if (child.getChildCount() != cols) {
          throw new SemanticException("Tables with different or invalid "
              + "number of keys in UNIQUEJOIN");
        }

        Vector<ASTNode> expressions = new Vector<ASTNode>();
        Vector<ASTNode> filt = new Vector<ASTNode>();
        ArrayList<ASTNode> filters = new ArrayList<ASTNode>();

        for (Node exp : child.getChildren()) {
          expressions.add((ASTNode) exp);
        }

        joinTree.getExpressions().add(expressions);
        joinTree.getFilters().add(filt);
        joinTree.getFiltersForPushing().add(filters);
        break;

      case HiveParser.KW_PRESERVE:
        lastPreserved = true;
        break;

      case HiveParser.TOK_SUBQUERY:
        throw new SemanticException(
            "Subqueries are not supported in UNIQUEJOIN");

      default:
        throw new SemanticException("Unexpected UNIQUEJOIN structure");
      }
    }

    joinTree.setBaseSrc(baseSrc.toArray(new String[0]));
    joinTree.setLeftAliases(leftAliases.toArray(new String[0]));
    joinTree.setRightAliases(rightAliases.toArray(new String[0]));

    joinCond[] condn = new joinCond[preserved.size()];
    for (int i = 0; i < condn.length; i++) {
      condn[i] = new joinCond(preserved.get(i));
    }
    joinTree.setJoinCond(condn);

    if (qb.getParseInfo().getHints() != null) {
      parseStreamTables(joinTree, qb);
    }

    return joinTree;
  }

  HashMap<String, HashSet<String>> _aliasMap = null;

  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinCond[] condn = new joinCond[1];

    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new joinCond(0, 1, joinType.LEFTSEMI);
      break;
    default:
      condn[0] = new joinCond(0, 1, joinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    LOG.debug("The join ast: " + joinParseTree.toString());

    joinTree.setJoinCond(condn);
    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    String fullAlias = null;
    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {

      String table_name = null;
      String userAlias = null;

      table_name = unescapeIdentifier(left.getChild(0).getChild(0).getText())
          .toLowerCase();

      if (left.getChildCount() != 1) {
        userAlias = unescapeIdentifier(
            left.getChild(left.getChildCount() - 1).getText()).toLowerCase();

        if (qb.getSubqForAlias(userAlias) != null) {
          fullAlias = userAlias;
        } else {
          fullAlias = qb.getTableRefFromUserAlias(userAlias).getDbName() + "/"
              + qb.getTableRefFromUserAlias(userAlias).tblName + "#"
              + userAlias;
        }

      } else {

        if ((left.getToken().getType() == HiveParser.TOK_TABREF)) {
          if (this.hasWith && left.getChild(0).getChildCount() == 1
              && this.withQueries.contains(table_name)) {
            fullAlias = table_name;
          } else {

            if (left.getChild(0).getChildCount() >= 2
                && ((ASTNode) left.getChild(0).getChild(
                    left.getChild(0).getChildCount() - 1)).getToken().getType() != HiveParser.TOK_PARTITIONREF
                && ((ASTNode) left.getChild(0).getChild(
                    left.getChild(0).getChildCount() - 1)).getToken().getType() != HiveParser.TOK_SUBPARTITIONREF
                && ((ASTNode) left.getChild(0).getChild(
                    left.getChild(0).getChildCount() - 1)).getToken().getType() != HiveParser.TOK_COMPPARTITIONREF) {
              LOG.debug("user has set db!");
              fullAlias = unescapeIdentifier(
                  left.getChild(0)
                      .getChild(left.getChild(0).getChildCount() - 1).getText())
                  .toLowerCase() + "/" + table_name;

            } else {
              fullAlias = SessionState.get().getDbName() + "/" + table_name;
            }
            if (qb.getUserAliasFromDBTB(fullAlias) != null) {
              if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
                throw new SemanticException("table : " + fullAlias
                    + " has more than one alias : "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                    + qb.getUserAliasFromDBTB(fullAlias).get(1));
              }

              if (left.getChildCount() != 1) {
                assert (unescapeIdentifier(
                    left.getChild(left.getChildCount() - 1).getText())
                    .toLowerCase().equalsIgnoreCase(qb.getUserAliasFromDBTB(
                    fullAlias).get(0)));
              }

              fullAlias = fullAlias + "#"
                  + qb.getUserAliasFromDBTB(fullAlias).get(0);
            }
          }
        } else
          fullAlias = unescapeIdentifier(left.getChild(0).getText())
              .toLowerCase();
      }

      LOG.debug("left full join alias is : " + fullAlias);

      String alias = fullAlias.toLowerCase();

      LOG.debug("left join alias is : " + alias);

      joinTree.setLeftAlias(alias);

      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
    } else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(qb, left);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++)
        leftAliases[i] = leftChildAliases[i];
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else
      assert (false);

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {

      String table_name = null;
      String userAlias = null;

      if (right.getChildCount() != 1) {
        userAlias = unescapeIdentifier(
            right.getChild(right.getChildCount() - 1).getText()).toLowerCase();

        if (qb.getSubqForAlias(userAlias) != null) {
          fullAlias = userAlias;
        } else {
          fullAlias = qb.getTableRefFromUserAlias(userAlias).getDbName() + "/"
              + qb.getTableRefFromUserAlias(userAlias).tblName + "#"
              + userAlias;
        }

      } else {

        if ((right.getToken().getType() == HiveParser.TOK_TABREF)) {
          table_name = unescapeIdentifier(
              right.getChild(0).getChild(0).getText()).toLowerCase();
          if (this.hasWith && right.getChild(0).getChildCount() == 1
              && this.withQueries.contains(table_name)) {
            fullAlias = table_name;
          } else {

            if (right.getChild(0).getChildCount() >= 2
                && ((ASTNode) right.getChild(0).getChild(
                    right.getChild(0).getChildCount() - 1)).getToken()
                    .getType() != HiveParser.TOK_PARTITIONREF
                && ((ASTNode) right.getChild(0).getChild(
                    right.getChild(0).getChildCount() - 1)).getToken()
                    .getType() != HiveParser.TOK_SUBPARTITIONREF
                && ((ASTNode) right.getChild(0).getChild(
                    right.getChild(0).getChildCount() - 1)).getToken()
                    .getType() != HiveParser.TOK_COMPPARTITIONREF) {
              fullAlias = unescapeIdentifier(
                  right.getChild(0)
                      .getChild(right.getChild(0).getChildCount() - 1)
                      .getText()).toLowerCase()
                  + "/" + table_name;

            } else {
              fullAlias = SessionState.get().getDbName() + "/" + table_name;
            }

            if (qb.getUserAliasFromDBTB(fullAlias) != null) {
              if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
                throw new SemanticException("table : " + fullAlias
                    + " has more than one alias : "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                    + qb.getUserAliasFromDBTB(fullAlias).get(1));
              }

              if (right.getChildCount() != 1) {
                assert (unescapeIdentifier(
                    right.getChild(right.getChildCount() - 1).getText())
                    .toLowerCase().equalsIgnoreCase(qb.getUserAliasFromDBTB(
                    fullAlias).get(0)));
              }

              fullAlias = fullAlias + "#"
                  + qb.getUserAliasFromDBTB(fullAlias).get(0);
            }
          }
        } else
          fullAlias = unescapeIdentifier(right.getChild(0).getText())
              .toLowerCase();

      }
      LOG.debug("right full join alias is : " + fullAlias);
      String alias = fullAlias.toLowerCase();

      LOG.debug("right join alias is : " + alias);
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null)
        children = new String[2];
      children[1] = alias;
      joinTree.setBaseSrc(children);
      if (joinTree.getNoSemiJoin() == false) {
        joinTree.addRHSSemijoin(alias);
      }
    } else
      assert false;

    Vector<Vector<ASTNode>> expressions = new Vector<Vector<ASTNode>>();
    expressions.add(new Vector<ASTNode>());
    expressions.add(new Vector<ASTNode>());
    joinTree.setExpressions(expressions);

    Vector<Vector<ASTNode>> filters = new Vector<Vector<ASTNode>>();
    filters.add(new Vector<ASTNode>());
    filters.add(new Vector<ASTNode>());
    joinTree.setFilters(filters);

    ArrayList<ArrayList<ASTNode>> filtersForPushing = new ArrayList<ArrayList<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);

    Vector<String> leftSrc = new Vector<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc, qb);
    if (leftSrc.size() == 1)
      joinTree.setLeftAlias(leftSrc.get(0));

    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equalsIgnoreCase(leftAlias))
            mapTable = true;
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equalsIgnoreCase(rightAlias))
            mapTable = true;
        }

        if (mapTable) {
          if (mapAliases == null) {
            mapAliases = new ArrayList<String>();
          }
          mapAliases.add(mapTbl.toLowerCase());
          joinTree.setMapSideJoin(true);
        }
      }

      joinTree.setMapAliases(mapAliases);

      parseStreamTables(joinTree, qb);
    } else {
      String _opt_switch_ = conf.get(ToolBox.CB_OPT_ATTR);
      if (_opt_switch_ == null || _opt_switch_.equals("")) {
      } else {
        try {
          int mapAliasesUplimit = 0;
          for (String _key : _aliasMap.keySet()) {
            mapAliasesUplimit += _aliasMap.get(_key).size();

          }

          List<String> mapSideTables = new ArrayList<String>();
          List<String> mapAliases = joinTree.getMapAliases();

          for (String _realTableName_ : _aliasMap.keySet()) {
            if (Hive.get().canTableMapJoin(_realTableName_) == Hive.mapjoinstat.canmapjoin) {
              mapSideTables.add(_realTableName_);
            }
          }

          for (String mapTbl : mapSideTables) {
            boolean mapTable = false;
            for (String _alias_ : _aliasMap.get(mapTbl)) {

              for (String leftAlias : joinTree.getLeftAliases()) {
                if (_alias_.equals(leftAlias))
                  mapTable = true;
              }
              for (String rightAlias : joinTree.getRightAliases()) {
                if (_alias_.equals(rightAlias))
                  mapTable = true;
              }

              if (mapTable) {
                joinTree.setMapSideJoin(true);
                if (mapAliases == null) {
                  mapAliases = new ArrayList<String>();
                }

                if (mapAliases.size() < mapAliasesUplimit - 1) {
                  mapAliases.add(_alias_.toLowerCase());
                } else
                  break;

              }
            }

            if (mapAliases != null
                && mapAliases.size() >= mapAliasesUplimit - 1)
              break;

          }

          joinTree.setMapAliases(mapAliases);

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    return joinTree;
  }

  private void parseStreamTables(QBJoinTree joinTree, QB qb) {
    List<String> streamAliases = joinTree.getStreamAliases();

    for (Node hintNode : qb.getParseInfo().getHints().getChildren()) {
      ASTNode hint = (ASTNode) hintNode;
      if (hint.getChild(0).getType() == HiveParser.TOK_STREAMTABLE) {
        for (int i = 0; i < hint.getChild(1).getChildCount(); i++) {
          if (streamAliases == null) {
            streamAliases = new ArrayList<String>();
          }
          streamAliases.add(hint.getChild(1).getChild(i).getText());
        }
      }
    }

    joinTree.setStreamAliases(streamAliases);
  }

  private void mergeJoins(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target, int pos) throws SemanticException {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length
        + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++)
      rightAliases[i] = trgtRightAliases[i];
    for (int i = 0; i < nodeRightAliases.length; i++)
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    target.setRightAliases(rightAliases);

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++)
      baseSrc[i] = trgtBaseSrc[i];
    for (int i = 1; i < nodeBaseSrc.length; i++)
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    target.setBaseSrc(baseSrc);

    Vector<Vector<ASTNode>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++)
      expr.add(node.getExpressions().get(i + 1));

    Vector<Vector<ASTNode>> filters = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filters.add(node.getFilters().get(i + 1));
      if (node.getFilters().get(0).size() != 0) {
        String re = "";
        for (ASTNode aa : node.getFilters().get(0)) {
          re += aa.dump();
        }
        throw new SemanticException("the join sql is not right: " + re
            + " is lost!");
      }
    }

    ArrayList<ArrayList<ASTNode>> filter = target.getFiltersForPushing();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filter.add(node.getFiltersForPushing().get(i + 1));
    }

    if (node.getFiltersForPushing().get(0).size() != 0) {
      ArrayList<ASTNode> filterPos = filter.get(pos);
      filterPos.addAll(node.getFiltersForPushing().get(0));
    }

    if (qb.getQbJoinTree() == node)
      qb.setQbJoinTree(node.getJoinSrc());
    else
      parent.setJoinSrc(node.getJoinSrc());

    if (node.getNoOuterJoin() && target.getNoOuterJoin())
      target.setNoOuterJoin(true);
    else
      target.setNoOuterJoin(false);

    if (node.getNoSemiJoin() && target.getNoSemiJoin())
      target.setNoSemiJoin(true);
    else
      target.setNoSemiJoin(false);

    target.mergeRHSSemijoin(node);

    joinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    joinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    joinCond[] newCondns = new joinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++)
      newCondns[i] = targetCondns[i];

    for (int i = 0; i < nodeCondnsSize; i++) {
      joinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0)
        nodeCondn.setLeft(pos);
      else
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
    if (target.isMapSideJoin()) {
      assert node.isMapSideJoin();
      List<String> mapAliases = target.getMapAliases();
      for (String mapTbl : node.getMapAliases())
        if (!mapAliases.contains(mapTbl))
          mapAliases.add(mapTbl.toLowerCase());
      target.setMapAliases(mapAliases);
    }
  }

  private int findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null)
      return -1;

    Vector<ASTNode> nodeCondn = node.getExpressions().get(0);
    Vector<ASTNode> targetCondn = null;

    if (leftAlias.equals(target.getLeftAlias())) {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    } else
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }

    if ((targetCondn == null) || (nodeCondn.size() != targetCondn.size()))
      return -1;

    for (int i = 0; i < nodeCondn.size(); i++)
      if (!nodeCondn.get(i).toStringTree()
          .equals(targetCondn.get(i).toStringTree()))
        return -1;

    return res;
  }

  private boolean mergeJoinNodes(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target) throws SemanticException {
    if (target == null)
      return false;

    int res = findMergePos(node, target);
    if (res != -1) {
      mergeJoins(qb, parent, node, target, res);
      return true;
    }

    return mergeJoinNodes(qb, parent, node, target.getJoinSrc());
  }

  private void mergeJoinTree(QB qb) throws SemanticException {
    QBJoinTree root = qb.getQbJoinTree();
    QBJoinTree parent = null;
    while (root != null) {
      boolean merged = mergeJoinNodes(qb, parent, root, root.getJoinSrc());

      if (parent == null) {
        if (merged)
          root = qb.getQbJoinTree();
        else {
          parent = root;
          root = root.getJoinSrc();
        }
      } else {
        parent = parent.getJoinSrc();
        root = parent.getJoinSrc();
      }
    }
  }

  private Operator insertSelectAllPlanForGroupBy(String dest, Operator input)
      throws SemanticException {
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Vector<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<exprNodeDesc> colList = new ArrayList<exprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new exprNodeColumnDesc(col.getType(), col.getInternalName(),
          col.getTabAlias(), col.getIsPartitionCol()));
      columnNames.add(col.getInternalName());
    }
    
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(colList, columnNames, true),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    
    if (input.getColumnExprMap() == null || input.getColumnExprMap().size() == 0)
      output.setColumnExprMap(input.getColumnExprMap());
    else {
      Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
      for (int i=0; i < colList.size(); i++) {
        String outputCol = getColumnInternalName(i);
        colExprMap.put(outputCol, colList.get(i));
      }
      output.setColumnExprMap(colExprMap);
    }
    
    return output;
  }

  private Operator insertSelectAllPlanForAnalysis(String dest, Operator input)
      throws SemanticException {
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Vector<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<exprNodeDesc> colList = new ArrayList<exprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();

    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new exprNodeColumnDesc(col.getType(), col.getInternalName(),
          col.getTabAlias(), col.getIsPartitionCol()));

      columnNames.add(col.getInternalName());
    }
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(colList, columnNames, true),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    output.setColumnExprMap(input.getColumnExprMap());

    return output;
  }

  private List<ASTNode> getCommonDistinctExprs(QB qb, Operator input) {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    if (ks.size() <= 1)
      return null;

    List<exprNodeDesc> oldList = null;
    List<ASTNode> oldASTList = null;

    for (String dest : ks) {
      Operator curr = input;

      if (qbp.getWhrForClause(dest) != null)
        return null;

      if (qbp.getAggregationExprsForClause(dest).size() == 0
          && getGroupByForClause(qbp, dest).size() == 0)
        return null;

      List<ASTNode> list = qbp.getDistinctFuncExprsForClause(dest);
      if (list.isEmpty())
        return null;

      List<exprNodeDesc> currDestList = new ArrayList<exprNodeDesc>();
      List<ASTNode> currASTList = new ArrayList<ASTNode>();
      for (ASTNode value : list) {
        try {
          for (int i = 1; i < value.getChildCount(); i++) {
            ASTNode parameter = (ASTNode) value.getChild(i);
            currDestList.add(genExprNodeDesc(parameter, inputRR, qb));
            currASTList.add(parameter);
          }
        } catch (SemanticException e) {
          return null;
        }
        if (oldList == null) {
          oldList = currDestList;
          oldASTList = currASTList;
        } else {
          if (oldList.size() != currDestList.size()) {
            return null;
          }
          for (int pos = 0; pos < oldList.size(); pos++) {
            if (!oldList.get(pos).isSame(currDestList.get(pos))) {
              return null;
            }
          }
        }
      }
    }

    return oldASTList;
  }

  private Operator createCommonReduceSink(QB qb, Operator input)
      throws SemanticException {
    List<ASTNode> distExprs = getCommonDistinctExprs(qb, input);

    QBParseInfo qbp = qb.getParseInfo();
    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    RowResolver inputRR = opParseCtx.get(input).getRR();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    List<String> outputColumnNames = new ArrayList<String>();
    for (ASTNode distn : distExprs) {
      exprNodeDesc distExpr = genExprNodeDesc(distn, inputRR, qb);
      reduceKeys.add(distExpr);
      String text = distn.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.putExpression(distn, colInfo);
        colExprMap.put(colInfo.getInternalName(), distExpr);
      }
    }

    for (String dest : ks) {

      List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
      for (int i = 0; i < grpByExprs.size(); ++i) {
        ASTNode grpbyExpr = grpByExprs.get(i);
        String text = grpbyExpr.toStringTree();

        if (reduceSinkOutputRowResolver.get("", text) == null) {
          exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR, qb);
          reduceValues.add(grpByExprNode);
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
              reduceValues.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
          outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
        }
      }

      HashMap<String, ASTNode> aggregationTrees = qbp
          .getAggregationExprsForClause(dest);
      assert (aggregationTrees != null);

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        String aggName = value.getChild(0).getText();

        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);
          String text = paraExpr.toStringTree();

          if (reduceSinkOutputRowResolver.get("", text) == null) {
            exprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRR, qb);
            reduceValues.add(paraExprNode);
            String field = Utilities.ReduceField.VALUE.toString() + "."
                + getColumnInternalName(reduceValues.size() - 1);
            ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
                reduceValues.size() - 1).getTypeInfo(), "", false);
            reduceSinkOutputRowResolver.putExpression(paraExpr, colInfo);
            outputColumnNames
                .add(getColumnInternalName(reduceValues.size() - 1));
          }
        }
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, reduceKeys.size(), -1),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), input),
        reduceSinkOutputRowResolver);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  @SuppressWarnings("nls")
  private Operator genBodyPlan(QB qb, Operator input) throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    boolean optimizeMultiGroupBy = (getCommonDistinctExprs(qb, input) != null);
    Operator curr = null;

    if (optimizeMultiGroupBy) {
      curr = createCommonReduceSink(qb, input);

      RowResolver currRR = opParseCtx.get(curr).getRR();
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(new forwardDesc(),
          new RowSchema(currRR.getColumnInfos()), curr), currRR);

      for (String dest : ks) {
        curr = input;
        curr = genGroupByPlan2MRMultiGroupBy(dest, qb, curr);
        curr = genSelectPlan(dest, qb, curr);
        Integer limit = qbp.getDestLimit(dest);
        if (limit != null) {
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), true);
          qb.getParseInfo().setOuterQueryLimit(limit.intValue());
        }
        curr = genFileSinkPlan(dest, qb, curr);

        filesinkop = curr;
        LOG.debug(dest);
      }
    } else {

      if (ks.size() > 1) {
        int testdistinct = 0;
        int testdistinctfunc = 0;
        int testgroupby = 0;
        int result = 0;
        for (String dest : ks) {
          if (getGroupByForClause(qbp, dest) != null) {
            testgroupby = getGroupByForClause(qbp, dest).size();
            LOG.debug("groupbysize1:  " + qb.getNumGbys());
            LOG.debug("groupbysize2:  " + testgroupby);
          }
          testdistinct = qb.getNumSelDi();
          if (!qbp.getDistinctFuncExprsForClause(dest).isEmpty()) {
            testdistinctfunc = 1;
          }
          if ((testgroupby > 0) && ((testdistinct + testdistinctfunc) > 0))
            result++;
        }
        if (result > 1) {
          conf.setVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE, "false");
        }
      }

      for (String dest : ks) {
        curr = input;

        if (qbp.getWhrForClause(dest) != null) {
          curr = genFilterPlan(dest, qb, curr);
        }

        if (qbp.getAggregationExprsForClause(dest).size() != 0
            || getGroupByForClause(qbp, dest).size() > 0) {

          curr = insertSelectAllPlanForGroupBy(dest, curr);
          if (qbp.getDestContainsGroupbyCubeOrRollupClause(dest)) {
            LOG.info("query contains cube/rollup/groupingsets and optimize it");
            if (conf.getVar(HiveConf.ConfVars.HIVECUBEROLLUPREDUCE)
                .equalsIgnoreCase("true")) {
              boolean skew = conf.getVar(
                  HiveConf.ConfVars.HIVECUBEROLLUPREDUCESKEW).equalsIgnoreCase(
                  "true");
              curr = insertCubeRollupReduceOp(dest, qb, curr, skew);
            }
            curr = insertRowExtendForCubeRollup(dest, qb, curr);
          }
          NewGroupByUtils1 newGroupByUtils = new NewGroupByUtils1();
          boolean usenewgroupby = conf.getBoolean("usenewgroupby", true);
          if (usenewgroupby) {
            newGroupByUtils.initialize(this.conf, this.opParseCtx);
          }
          if (conf.getVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)
              .equalsIgnoreCase("true")) {

            if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
                .equalsIgnoreCase("false")) {
              String _opt_switch_ = conf.get(ToolBox.CB_OPT_ATTR);
              if (_opt_switch_ == null || _opt_switch_.equals("")) {
                if (usenewgroupby) {
                  curr = newGroupByUtils.genGroupByPlanMapAggr1MR(dest, qb,
                      curr);
                } else {
                  curr = genGroupByPlanMapAggr1MR(dest, qb, curr);
                }
              } else {
                try {

                  if (_plannerInnerMost_) {
                    _plannerInnerMost_ = false;
                    Hive.skewstat groupBySkewStat = Hive.skewstat.skew;
                    for (ToolBox.tableTuple _groupByTuple : _groupByKeys) {
                      if (Hive.skewstat.noskew == Hive.get().getSkew(
                          _tableAliasTuple_.getTableName(),
                          _groupByTuple.getFieldName())) {
                        groupBySkewStat = Hive.skewstat.noskew;
                        break;
                      }

                    }
                    Hive.skewstat distinctSkewStat = Hive.skewstat.skew;
                    for (ToolBox.tableDistinctTuple _distinctTuple : _distinctKeys) {
                      if (Hive.skewstat.noskew == Hive.get().getSkew(
                          _tableAliasTuple_.getTableName(),
                          _distinctTuple.getDistinctField())) {
                        distinctSkewStat = Hive.skewstat.noskew;
                        break;
                      }

                    }

                    if (groupBySkewStat == Hive.skewstat.skew
                        && distinctSkewStat == Hive.skewstat.noskew) {
                      if (usenewgroupby) {
                        curr = newGroupByUtils.genGroupByPlanMapAggr2MR(dest,
                            qb, curr);
                      } else {
                        curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
                      }
                    } else {
                      if (usenewgroupby) {
                        curr = newGroupByUtils.genGroupByPlanMapAggr1MR(dest,
                            qb, curr);
                      } else {
                        curr = genGroupByPlanMapAggr1MR(dest, qb, curr);
                      }
                    }
                  } else {
                    if (usenewgroupby) {
                      curr = newGroupByUtils.genGroupByPlanMapAggr1MR(dest, qb,
                          curr);
                    } else {
                      curr = genGroupByPlanMapAggr1MR(dest, qb, curr);
                    }
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }

            } else if (usenewgroupby) {
              curr = newGroupByUtils.genGroupByPlanMapAggr2MR(dest, qb, curr);
            } else {
              curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
            }
          } else if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
              .equalsIgnoreCase("true")) {
            if (usenewgroupby) {
              curr = newGroupByUtils.genGroupByPlan2MR(dest, qb, curr);
            } else {
              curr = genGroupByPlan2MR(dest, qb, curr);
            }
          } else {
            if (usenewgroupby) {
              curr = newGroupByUtils.genGroupByPlan1MR(dest, qb, curr);
            } else {
              curr = genGroupByPlan1MR(dest, qb, curr);
            }
          }
        }

        if (qbp.getAnalysisExprsForClause(dest).size() != 0) {

          curr = genAnalysisPlan1MR(dest, qb, curr);
        }

        if (qbp.getHavingForClause(dest) != null) {
          if (getGroupByForClause(qbp, dest).size() == 0) {
            throw new SemanticException("HAVING specified without GROUP BY");
          }
          curr = genHavingPlan(dest, qb, curr);
        }

        curr = genSelectPlan(dest, qb, curr);

        Integer limit = qbp.getDestLimit(dest);

        boolean needHalfSort = false;
        ASTNode OrderByExprs = qbp.getOrderByForClause(dest);
        if (OrderByExprs != null) {
          if (limit == null) {
          } else if (limit > conf.getIntVar(HiveConf.ConfVars.MAXLIMITCOUNT)) {
            throw new SemanticException(
                ErrorMsg.INVALID_LIMIT_COUNT_FOR_ORDERBY.getMsg(OrderByExprs));
          } else {
            needHalfSort = true;
          }
        }

        if (needHalfSort) {
          curr = genHalfSortPlan(dest, qb, curr, limit.intValue());
        }

        if (qbp.getClusterByForClause(dest) != null
            || qbp.getDistributeByForClause(dest) != null
            || qbp.getOrderByForClause(dest) != null
            || qbp.getSortByForClause(dest) != null) {

          int numReducers = -1;

          if (qbp.getOrderByForClause(dest) != null)
            numReducers = 1;

          curr = genReduceSinkPlan(dest, qb, curr, numReducers);
        }

        if (qbp.getIsSubQ()) {
          if (needHalfSort) {
            curr = genHalfSortPlan(dest, qb, curr, limit.intValue());
          } else {
            if (limit != null) {
              curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
                  qbp.getOrderByForClause(dest) != null ? false : true);
            }
          }
        } else {
          curr = genConversionOps(dest, qb, curr);

          if (needHalfSort) {
            curr = genHalfSortPlan(dest, qb, curr, limit.intValue());
          } else {
            if (limit != null) {
              boolean extraMRStep = true;

              if (qb.getIsQuery() && qbp.getClusterByForClause(dest) == null
                  && qbp.getSortByForClause(dest) == null)
                extraMRStep = false;

              curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
                  extraMRStep);
              qb.getParseInfo().setOuterQueryLimit(limit.intValue());
            }
          }
          curr = genFileSinkPlan(dest, qb, curr);

          filesinkop = curr;
          LOG.debug(dest);
        }

        if (qb.getParseInfo().getAlias() != null) {
          RowResolver rr = opParseCtx.get(curr).getRR();
          if (rr.checkCols() == false) {
            throw new SemanticException("subquery lost col!");
          }
          RowResolver newRR = new RowResolver();
          String alias = qb.getParseInfo().getAlias();
          for (ColumnInfo colInfo : rr.getColumnInfos()) {
            String name = colInfo.getInternalName();
            String[] tmp = rr.reverseLookup(name);
            newRR.put(alias, tmp[1], colInfo);
          }
          opParseCtx.get(curr).setRR(newRR);
        }
      }
    }

    LOG.debug("Created Body Plan for Query Block " + qb.getId());
    return curr;
  }

  private Operator insertCubeRollupReduceOp(String dest, QB qb, Operator input,
      boolean skew) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    RowResolver cubeRollupReduceRowResolver = new RowResolver();
    cubeRollupReduceRowResolver.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> values = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();

    int colid = 0;

    ArrayList<ASTNode> groupByKeysAsts = qb.getParseInfo()
        .getDestToGroupbyNodes(dest);
    for (int i = 0; i < groupByKeysAsts.size(); i++) {
      ASTNode grpbyExpr = groupByKeysAsts.get(i);
      if (cubeRollupReduceRowResolver.getExpression(grpbyExpr) == null) {
        exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR, qb);
        groupByKeys.add(grpByExprNode);
        String colName = getColumnInternalName(colid++);
        outputColumnNames.add(colName);
        String field = Utilities.ReduceField.KEY.toString() + "." + colName;
        ColumnInfo colInfo = new ColumnInfo(field, grpByExprNode.getTypeInfo(),
            "", false);
        cubeRollupReduceRowResolver.putExpression(grpbyExpr, colInfo);
        colExprMap.put(field, grpByExprNode);
      }
    }

    HashMap<String, ASTNode> aggregationTrees = qb.getParseInfo()
        .getAggregationExprsForClause(dest);

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        if (cubeRollupReduceRowResolver.getExpression(paraExpr) == null) {
          exprNodeDesc paraExprInfo = genExprNodeDesc(paraExpr, inputRR, qb);

          values.add(paraExprInfo);
          String colName = getColumnInternalName(colid++);
          outputColumnNames.add(colName);
          String field = Utilities.ReduceField.VALUE.toString() + "." + colName;
          ColumnInfo colInfo = new ColumnInfo(field,
              paraExprInfo.getTypeInfo(), "", false);
          cubeRollupReduceRowResolver.putExpression(paraExpr, colInfo);
          colExprMap.put(field, paraExprInfo);
        }
      }
    }

    ASTNode grpByExprs = qb.getParseInfo().getGroupByForClause(dest);
    int groupKeyRownumberWhole = 1;
    for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
      ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
      ArrayList<Integer> grpTags = getGrpColTags(grpbyExpr);
      groupKeyRownumberWhole = groupKeyRownumberWhole * grpTags.size();
    }

    reduceSinkDesc rsdesc = PlanUtils.getReduceSinkDesc(groupByKeys, values,
        outputColumnNames, true, -1, skew ? -1 : Integer.MAX_VALUE, -1);
    double threshould = Double.parseDouble(conf
        .getVar(HiveConf.ConfVars.HIVECUBEROLLUPREDUCETHRESHOULD));
    rsdesc.setReduceNumFactor((int) Math.max(1, threshould
        * groupKeyRownumberWhole));

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(rsdesc, new RowSchema(
            cubeRollupReduceRowResolver.getColumnInfos()), input),
        cubeRollupReduceRowResolver);
    rsOp.setColumnExprMap(colExprMap);

    return rsOp;
  }

  private Operator insertRowExtendForCubeRollup(String dest, QB qb,
      Operator input) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    RowResolver rowExtendOutputRowResolver = new RowResolver();
    rowExtendOutputRowResolver.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> values = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();

    ASTNode grpByExprs = qb.getParseInfo().getGroupByForClause(dest);

    int groupNumber = grpByExprs.getChildCount();
    ArrayList<Integer> groupKeyRowNumbers = new ArrayList<Integer>(groupNumber);
    ArrayList<Integer> groupKeyColSizes = new ArrayList<Integer>(groupNumber);
    ArrayList<ArrayList<Integer>> groupColTags = new ArrayList<ArrayList<Integer>>(
        groupNumber);

    int groupKeyRownumberWhole = 1;
    for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
      ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
      ArrayList<ASTNode> grpes = getGrpExprs(grpbyExpr);
      groupKeyColSizes.add(grpes.size());
      ArrayList<Integer> grpTags = getGrpColTags(grpbyExpr);
      groupColTags.add(grpTags);
      groupKeyRowNumbers.add(grpTags.size());
      groupKeyRownumberWhole = groupKeyRownumberWhole * grpTags.size();
    }
    int[] indexbase = new int[groupNumber];
    indexbase[groupNumber - 1] = 1;
    for (int i = groupNumber - 2; i >= 0; i--) {
      indexbase[i] = indexbase[i + 1] * groupKeyRowNumbers.get(i + 1);
    }
    ArrayList<Long> tagids = new ArrayList<Long>(groupKeyRownumberWhole);
    for (int i = 0; i < groupKeyRownumberWhole; i++) {
      long id = 0;
      int ii = i;
      int currlen = 0;
      for (int j = 0; j < groupNumber; j++) {
        int colSize = groupKeyColSizes.get(j);
        long mask = (1l << colSize) - 1;
        int gid = groupColTags.get(j).get(ii / indexbase[j]);
        id |= ((gid & mask) << currlen);
        ii %= indexbase[j];
        currlen += colSize;
      }
      tagids.add(id);
      LOG.debug("cube_rollup_id: " + id);
    }

    List<ASTNode> groupByKeysAsts = qb.getParseInfo().getDestToGroupbyNodes(
        dest);
    int colid = 0;

    ArrayList<ASTNode> groupings = new ArrayList<ASTNode>();
    groupings
        .addAll(qb.getParseInfo().getGroupingExprsForClause(dest).values());
    ArrayList<Integer> groupingColOffs = new ArrayList<Integer>();
    if (groupings != null) {
      for (int i = 0; i < groupings.size(); i++) {
        ASTNode groupingExpr = groupings.get(i);
        String colName = getColumnInternalName(colid++);
        outputColumnNames.add(colName);
        ColumnInfo colInfo = new ColumnInfo(colName,
            TypeInfoFactory.intTypeInfo, "", false);
        rowExtendOutputRowResolver.putExpression(groupingExpr, colInfo);
        String strTree = groupingExpr.getChild(1).toStringTree();
        for (int j = 0; j < groupByKeysAsts.size(); j++) {
          if (groupByKeysAsts.get(j).toStringTree().equalsIgnoreCase(strTree)) {
            groupingColOffs.add(j);
            break;
          }
        }
      }
    }

    String colName = getColumnInternalName(colid++);
    outputColumnNames.add(colName);
    ColumnInfo info = new ColumnInfo(colName, TypeInfoFactory.longTypeInfo, "",
        false);

    rowExtendOutputRowResolver.put("",
        NewGroupByUtils1._CUBE_ROLLUP_GROUPINGSETS_TAG_, info);

    HashMap<Integer, Integer> gbkIdx2compactIdx = new HashMap<Integer, Integer>();
    for (int i = 0; i < groupByKeysAsts.size(); i++) {
      ASTNode grpbyExpr = groupByKeysAsts.get(i);
      if (rowExtendOutputRowResolver.getExpression(grpbyExpr) == null) {
        exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR, qb);
        gbkIdx2compactIdx.put(i, groupByKeys.size());
        groupByKeys.add(grpByExprNode);
        colName = getColumnInternalName(colid++);
        outputColumnNames.add(colName);
        ColumnInfo colInfo = new ColumnInfo(colName,
            grpByExprNode.getTypeInfo(), "", false);
        rowExtendOutputRowResolver.putExpression(grpbyExpr, colInfo);
        colExprMap.put(colName, grpByExprNode);
      } else {
        String str = grpbyExpr.toStringTree();
        for (int j = 0; j < i; j++) {
          if (groupByKeysAsts.get(j).toStringTree().equalsIgnoreCase(str)) {
            gbkIdx2compactIdx.put(i, gbkIdx2compactIdx.get(j));
            break;
          }
        }
      }
    }

    HashMap<String, ASTNode> aggregationTrees = qb.getParseInfo()
        .getAggregationExprsForClause(dest);

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        if (rowExtendOutputRowResolver.getExpression(paraExpr) == null) {
          exprNodeDesc paraExprInfo = genExprNodeDesc(paraExpr, inputRR, qb);

          values.add(paraExprInfo);
          colName = getColumnInternalName(colid++);
          outputColumnNames.add(colName);
          ColumnInfo colInfo = new ColumnInfo(colName,
              paraExprInfo.getTypeInfo(), "", false);
          rowExtendOutputRowResolver.putExpression(paraExpr, colInfo);
          colExprMap.put(colName, paraExprInfo);
        }
      }
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new rowExtendDesc(groupingColOffs, groupByKeys, tagids,
            gbkIdx2compactIdx, values, outputColumnNames), new RowSchema(
            rowExtendOutputRowResolver.getColumnInfos()), input),
        rowExtendOutputRowResolver);
    output.setColumnExprMap(colExprMap);
    return output;

  }

  private ArrayList<Integer> getGrpColTags(ASTNode grpbyExpr) {
    ArrayList<Integer> tags = new ArrayList<Integer>();
    ArrayList<Integer> groupsizes = new ArrayList<Integer>();
    if (grpbyExpr.getType() == HiveParser.TOK_CUBE
        || grpbyExpr.getType() == HiveParser.TOK_ROLLUP
        || grpbyExpr.getType() == HiveParser.TOK_GROUPINGSETS) {
      for (int i = 0; i < grpbyExpr.getChildCount(); i++) {
        ASTNode child = (ASTNode) grpbyExpr.getChild(i);
        if (child.getType() == HiveParser.TOK_GROUP) {
          groupsizes.add(child.getChildCount());
        } else {
          groupsizes.add(1);
        }
      }
    }

    if (grpbyExpr.getType() == HiveParser.TOK_CUBE) {
      int cnt = grpbyExpr.getChildCount();
      int num = 1 << cnt;
      for (int i = 0; i < num; i++) {
        int tag = i;
        for (int j = cnt - 1; j >= 0; j--) {
          tag = insertBit(tag, j, groupsizes.get(j));
        }
        tags.add(tag);
      }
    } else if (grpbyExpr.getType() == HiveParser.TOK_ROLLUP) {
      tags.add(0);
      int size = 0;
      for (int i = 0; i < groupsizes.size(); i++) {
        size += groupsizes.get(i);
        tags.add((1 << size) - 1);
      }
    } else if (grpbyExpr.getType() == HiveParser.TOK_GROUPINGSETS) {
      int size = 0;
      for (int i = 0; i < groupsizes.size(); i++) {
        int size1 = groupsizes.get(i);
        tags.add(((1 << size1) - 1) << size);
        size += size1;
      }
    } else {
      tags.add(1);
    }
    return tags;
  }

  private int insertBit(int tag, int pos, int size) {
    if (size > 1) {
      int l = (tag << (size - 1)) & ((-1) << (pos + size));
      int bit = (tag >> pos) & 1;
      int m = (bit == 0) ? 0 : (((1 << size) - 1) << pos);
      int r = tag & ((1 << pos) - 1);
      tag = l | m | r;
    }
    return tag;
  }

  private ArrayList<ASTNode> getGrpExprs(ASTNode grpbyExpr) {
    ArrayList<ASTNode> exprs = new ArrayList<ASTNode>();
    if (grpbyExpr.getType() == HiveParser.TOK_CUBE
        || grpbyExpr.getType() == HiveParser.TOK_ROLLUP
        || grpbyExpr.getType() == HiveParser.TOK_GROUPINGSETS) {
      for (int i = 0; i < grpbyExpr.getChildCount(); i++) {
        ASTNode child = (ASTNode) grpbyExpr.getChild(i);
        if (child.getType() == HiveParser.TOK_GROUP) {
          for (int j = 0; j < child.getChildCount(); j++) {
            exprs.add((ASTNode) child.getChild(j));
          }
        } else {
          exprs.add(grpbyExpr);
        }
      }
    } else {
      exprs.add(grpbyExpr);
    }
    return exprs;
  }

  @SuppressWarnings("nls")
  private Operator genUnionPlan(String unionalias, String leftalias,
      Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    RowResolver leftRR = opParseCtx.get(leftOp).getRR();
    RowResolver rightRR = opParseCtx.get(rightOp).getRR();

    HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    ArrayList<ColumnInfo> leftlist = leftRR.getFieldList(leftalias);
    ArrayList<ColumnInfo> rightlist = rightRR.getFieldList(rightalias);

    if (leftlist.size() != rightlist.size()) {
      throw new SemanticException(
          "Schema of both sides of union should match. "
              + " does not have same field number " + leftlist.size() + ":"
              + rightlist.size());
    }
    for (int i = 0; i < leftlist.size(); i++) {
      LOG.debug("Union all LEFT TYPE:" + leftlist.get(i).getType().getTypeName());
      LOG.debug("Union all RIGHT TYPE:"
          + rightlist.get(i).getType().getTypeName());

      if (!leftlist.get(i).getType().getTypeName().equals("void")
          && rightlist.get(i).getType().getTypeName().equals("void")) {

        rightlist.get(i).setType(leftlist.get(i).getType());

      } else if (leftlist.get(i).getType().getTypeName().equals("void")
          && !rightlist.get(i).getType().getTypeName().equals("void")) {

        leftlist.get(i).setType(rightlist.get(i).getType());

      } else

      if (leftlist.get(i).getType().getTypeName().equals("int")
          && rightlist.get(i).getType().getTypeName().equals("bigint")) {
        leftlist.get(i).setType(rightlist.get(i).getType());
      }

      if (leftlist.get(i).getType().getTypeName().equals("bigint")
          && rightlist.get(i).getType().getTypeName().equals("int")) {
        rightlist.get(i).setType(leftlist.get(i).getType());
      }

      if ((leftlist.get(i).getType().getTypeName().equals("int") || leftlist
          .get(i).getType().getTypeName().equals("bigint"))
          && rightlist.get(i).getType().getTypeName().equals("string")) {
        leftlist.get(i).setType(rightlist.get(i).getType());
      }
      if (leftlist.get(i).getType().getTypeName().equals("string")
          && (rightlist.get(i).getType().getTypeName().equals("int") || rightlist
              .get(i).getType().getTypeName().equals("bigint"))) {
        rightlist.get(i).setType(leftlist.get(i).getType());
      }

      if (!leftlist.get(i).getType().getTypeName()
          .equals(rightlist.get(i).getType().getTypeName())) {
        throw new SemanticException(
            "Schema of both sides of union should match: Column "
                + " is of type " + leftlist.get(i).getType().getTypeName()
                + " on first table and type "
                + rightlist.get(i).getType().getTypeName() + " on second table");
      }

    }

    RowResolver unionoutRR = new RowResolver();
    for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
    }

    for (ColumnInfo info : leftRR.getColumnInfos()) {
      if (info.getAlias() == null)
        isFixUnionAndSelectStar = false;
    }
    if (isFixUnionAndSelectStar) {
      isFixUniqueUnionAndSelectStar = true;
      for (ColumnInfo info : leftRR.getColumnInfos()) {
        String field = info.getAlias();
        unionoutRR.put(unionalias, field, info);
      }
    } else {
      for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
        String field = lEntry.getKey();
        ColumnInfo lInfo = lEntry.getValue();
        unionoutRR.put(unionalias, field, lInfo);
      }
    }
    isFixUnionAndSelectStar = false;

    if ((leftOp instanceof UnionOperator) || (rightOp instanceof UnionOperator)) {
      if (leftOp instanceof UnionOperator) {
        List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
        child.add(leftOp);
        rightOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = leftOp
            .getParentOperators();

        if (parent != null && parent.size() != 0) {
          for (Operator item : parent) {
            RowResolver itemRR = opParseCtx.get(item).getRR();
            Set<String> nameSet = itemRR.getRslvNameSet();
            if (nameSet != null && nameSet.size() != 0) {
              ArrayList<ColumnInfo> destColList = unionoutRR
                  .getFieldList(unionalias);
              for (String name : nameSet) {
                ArrayList<ColumnInfo> colList = itemRR.getFieldList(name);
                if (destColList.size() == colList.size()) {
                  for (int i = 0; i < colList.size(); i++) {
                    colList.get(i).setType(destColList.get(i).getType());
                  }
                }
              }
            }
          }
        }
        parent.add(rightOp);

        unionDesc uDesc = ((UnionOperator) leftOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs() + 1);
        return putOpInsertMap(leftOp, unionoutRR);
      } else {
        List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
        child.add(rightOp);
        leftOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = rightOp
            .getParentOperators();

        if (parent != null && parent.size() != 0) {
          for (Operator item : parent) {
            RowResolver itemRR = opParseCtx.get(item).getRR();
            Set<String> nameSet = itemRR.getRslvNameSet();
            if (nameSet != null && nameSet.size() != 0) {
              ArrayList<ColumnInfo> destColList = unionoutRR
                  .getFieldList(unionalias);
              for (String name : nameSet) {
                ArrayList<ColumnInfo> colList = itemRR.getFieldList(name);
                if (destColList.size() == colList.size()) {
                  for (int i = 0; i < colList.size(); i++) {
                    colList.get(i).setType(destColList.get(i).getType());
                  }
                }
              }
            }
          }
        }

        parent.add(leftOp);
        unionDesc uDesc = ((UnionOperator) rightOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs() + 1);

        return putOpInsertMap(rightOp, unionoutRR);
      }
    }

    Operator<? extends Serializable> unionforward = OperatorFactory
        .getAndMakeChild(new unionDesc(),
            new RowSchema(unionoutRR.getColumnInfos()));

    List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    rightOp.setChildOperators(child);

    child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    leftOp.setChildOperators(child);

    List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
    parent.add(leftOp);
    parent.add(rightOp);
    unionforward.setParentOperators(parent);

    return putOpInsertMap(unionforward, unionoutRR);
  }

  @SuppressWarnings("nls")
  private Operator genUniqueUnionReduceSinkOperater(String unionalias,
      Operator inputOp) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(inputOp).getRR();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(false);

    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    int keyPos = 0;
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      reduceKeys.add(new exprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsPartitionCol()));
      colExprMap.put(colInfo.getInternalName(),
          reduceKeys.get(reduceKeys.size() - 1));
      String colName = getColumnInternalName(keyPos++);
      outputColumnNames.add(colName);
      reduceSinkOutputRowResolver.put(unionalias, colName, colInfo);

    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, 0, reduceKeys.size(), -1),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
            inputOp), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);

    LOG.debug("gen reduce sink op for unique union "
        + reduceSinkOutputRowResolver.toString());

    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
            Utilities.ReduceField.KEY.toString(), "", false)), new RowSchema(
            out_rwsch.getColumnInfos()), rsOp), out_rwsch);

    LOG.debug("Created ReduceSink Plan for row schema: " + out_rwsch.toString());
    return output;
  }

  @SuppressWarnings("nls")
  private Operator genUniqueUnionPlan(String unionalias, String leftalias,
      Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    Operator unionforward = genUnionPlan(unionalias, leftalias, leftOp,
        rightalias, rightOp);
    LOG.info("genUnionPlan OK");
    Operator uniquereducesink = genUniqueUnionReduceSinkOperater(unionalias,
        unionforward);
    LOG.info("genUniqueUnionReduceSinkOperater OK");
    RowResolver uniqueUnionInputRR = opParseCtx.get(uniquereducesink).getRR();

    ArrayList<exprNodeDesc> keyDesc = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    HashMap<String, ColumnInfo> map = uniqueUnionInputRR
        .getFieldMap(unionalias);

    if (isFixUniqueUnionAndSelectStar) {
      int outputPos = 0;
      for (ColumnInfo tcol : uniqueUnionInputRR.getColumnInfos()) {
        keyDesc.add(new exprNodeColumnDesc(tcol.getType(), tcol
            .getInternalName(), tcol.getTabAlias(), tcol.getIsPartitionCol()));
        outputColumnNames.add(getColumnInternalName(outputPos++));

      }
      isFixUniqueUnionAndSelectStar = false;
    } else {
      Iterator<String> fNamesIter = map.keySet().iterator();
      int outputPos = 0;
      while (fNamesIter.hasNext()) {
        String field = fNamesIter.next();
        LOG.debug("new field name :" + field);
        ColumnInfo valueInfo = uniqueUnionInputRR.get(unionalias, field);
        keyDesc.add(new exprNodeColumnDesc(valueInfo.getType(), valueInfo
            .getInternalName(), valueInfo.getTabAlias(), valueInfo
            .getIsPartitionCol()));
        LOG.debug("new keyDesc  InternalName:" + valueInfo.getInternalName());
        outputColumnNames.add(getColumnInternalName(outputPos++));

      }
    }

    Operator<? extends Serializable> uniqueUnionForward = OperatorFactory
        .getAndMakeChild(new uniqueDesc(keyDesc, outputColumnNames),
            new RowSchema(uniqueUnionInputRR.getColumnInfos()),
            uniquereducesink);

    LOG.debug("genUniqueUnionReduceSinkOperater OK");
    return putOpInsertMap(uniqueUnionForward, uniqueUnionInputRR);

  }

  private exprNodeDesc genSamplePredicate(TableSample ts,
      List<String> bucketCols, boolean useBucketCols, String alias,
      RowResolver rwsch, QBMetaData qbm, exprNodeDesc planExpr)
      throws SemanticException {

    exprNodeDesc numeratorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getNumerator() - 1));

    exprNodeDesc denominatorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getDenominator()));

    exprNodeDesc intMaxExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(Integer.MAX_VALUE));

    ArrayList<exprNodeDesc> args = new ArrayList<exprNodeDesc>();
    if (planExpr != null)
      args.add(planExpr);
    else if (useBucketCols) {
      for (String col : bucketCols) {
        ColumnInfo ci = rwsch.get(alias, col);
        args.add(new exprNodeColumnDesc(ci.getType(), ci.getInternalName(), ci
            .getTabAlias(), ci.getIsPartitionCol()));
      }
    } else {
      for (ASTNode expr : ts.getExprs()) {
        args.add(genExprNodeDesc(expr, rwsch, qb));
      }
    }

    exprNodeDesc equalsExpr = null;
    {
      exprNodeDesc hashfnExpr = new exprNodeGenericFuncDesc(
          TypeInfoFactory.intTypeInfo, new GenericUDFHash(), args);
      assert (hashfnExpr != null);
      LOG.debug("hashfnExpr = " + hashfnExpr);
      exprNodeDesc andExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
      assert (andExpr != null);
      LOG.debug("andExpr = " + andExpr);
      exprNodeDesc modExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("%", andExpr, denominatorExpr);
      assert (modExpr != null);
      LOG.debug("modExpr = " + modExpr);
      LOG.debug("numeratorExpr = " + numeratorExpr);
      equalsExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", modExpr, numeratorExpr);
      LOG.debug("equalsExpr = " + equalsExpr);
      assert (equalsExpr != null);
    }
    return equalsExpr;
  }

  @SuppressWarnings("nls")
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
    TablePartition tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;

    Operator<? extends Serializable> top = this.topOps.get(alias_id);
    Operator<? extends Serializable> dummySel = this.topSelOps.get(alias_id);
    if (dummySel != null)
      top = dummySel;

    if (top == null) {
      rwsch = new RowResolver();

      Set<String> partFieldNames = null;
      List<FieldSchema> partFields = tab.getPartCols();
      if (partFields != null) {
        partFieldNames = new HashSet<String>();
        for (FieldSchema field : partFields) {
          partFieldNames.add(field.getName());
        }
      }
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
            .getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          String fieldName = fields.get(i).getFieldName();

          boolean isPartField = false;
          if (partFieldNames != null) {
            isPartField = partFieldNames.contains(fieldName);
          }

          rwsch.put(
              alias,
              fieldName,
              new ColumnInfo(fieldName, TypeInfoUtils
                  .getTypeInfoFromObjectInspector(fields.get(i)
                      .getFieldObjectInspector()), alias, isPartField));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }

      top = putOpInsertMap(
          OperatorFactory.get(new tableScanDesc(alias),
              new RowSchema(rwsch.getColumnInfos())), rwsch);

      this.topOps.put(alias_id, top);

      this.topToTable.put((TableScanOperator) top, tab);
    } else {
      rwsch = opParseCtx.get(top).getRR();
      top.setChildOperators(null);
    }

    Operator<? extends Serializable> tableOp = top;
    TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      ArrayList<ASTNode> sampleExprs = ts.getExprs();

      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();

      if (tabBucketCols.size() == 0 && sampleExprs.size() == 0) {
        throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " "
            + tab.getName());
      }

      boolean colsEqual = true;
      if ((sampleExprs != null) && (tabBucketCols != null) && 
          (sampleExprs.size() != tabBucketCols.size()) && (sampleExprs.size() != 0)) {
        colsEqual = false;
      }

      if (sampleExprs != null) {
        for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
          boolean colFound = false;
          for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
            if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_TABLE_OR_COL) {
              break;
            }
  
            if (((ASTNode) sampleExprs.get(i).getChild(0)).getText()
                .equalsIgnoreCase(tabBucketCols.get(j))) {
              colFound = true;
            }
          }
          colsEqual = (colsEqual && colFound);
        }
      }

      ts.setInputPruning((sampleExprs == null || sampleExprs.size() == 0 || colsEqual));

      if ((sampleExprs == null || sampleExprs.size() == 0 || colsEqual)
          && (num == den || den <= numBuckets && numBuckets % den == 0)) {
        LOG.debug("No need for sample filter");
        exprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new filterDesc(
            samplePredicate, true), top);
      } else {
        LOG.debug("Need sample filter");
        exprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new filterDesc(
            samplePredicate, true), top);
      }
    } else {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      if (testMode) {
        String tabName = tab.getName();

        String unSampleTblList = conf
            .getVar(HiveConf.ConfVars.HIVETESTMODENOSAMPLE);
        String[] unSampleTbls = unSampleTblList.split(",");
        boolean unsample = false;
        for (String unSampleTbl : unSampleTbls)
          if (tabName.equalsIgnoreCase(unSampleTbl))
            unsample = true;

        if (!unsample) {
          int numBuckets = tab.getNumBuckets();

          if (numBuckets > 0) {
            TableSample tsSample = new TableSample(1, numBuckets);
            tsSample.setInputPruning(true);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.debug("No need for sample filter");
          } else {
            int freq = conf.getIntVar(HiveConf.ConfVars.HIVETESTMODESAMPLEFREQ);
            TableSample tsSample = new TableSample(1, freq);
            tsSample.setInputPruning(false);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.debug("Need sample filter");
            exprNodeDesc randFunc = TypeCheckProcFactory.DefaultExprProcessor
                .getFuncExprNodeDesc("rand",
                    new exprNodeConstantDesc(Integer.valueOf(460476415)));
            exprNodeDesc samplePred = genSamplePredicate(tsSample, null, false,
                alias, rwsch, qb.getMetaData(), randFunc);
            tableOp = OperatorFactory.getAndMakeChild(new filterDesc(
                samplePred, true), top);
          }
        }
      }
    }

    Operator output = putOpInsertMap(tableOp, rwsch);
    LOG.debug("Created Table Plan for " + alias + " " + tableOp.toString());

    return output;
  }

  private Operator genPlan(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      return genPlan(qbexpr.getQB());
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      Operator qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(qbexpr.getQBExpr2());
      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNIQUE_UNION) {
      Operator qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(qbexpr.getQBExpr2());
      return genUniqueUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1()
          .getAlias(), qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  private boolean isCartesianProduct(QB qb) {
    if (qb == null) {
      return false;
    }

    QBJoinTree qbTree = qb.getQbJoinTree();
    boolean isCP = isCartesianProduct(qbTree);

    if (isCP) {
      return true;
    }

    Set<String> subqNames = qb.getSubqAliases();
    if (subqNames != null && !subqNames.isEmpty()) {
      for (String subqName : subqNames) {
        QBExpr qbExpr = qb.getSubqForAlias(subqName);
        boolean isSCP = isCartesianProduct(qbExpr.getQB());
        if (isSCP) {
          return true;
        }
      }

    }

    return false;
  }

  private boolean isCartesianProduct(QBJoinTree qbTree) {
    if (qbTree != null) {
      Vector<Vector<ASTNode>> expressions = qbTree.getExpressions();
      if (expressions != null && !expressions.isEmpty()) {
        for (Vector<ASTNode> expression : expressions) {
          if (expression == null || expression.isEmpty()) {
            return true;
          }
        }
      } else {
        return true;
      }
    } else {
      return false;
    }

    return isCartesianProduct(qbTree.getJoinSrc());
  }

  @SuppressWarnings("nls")
  public Operator genPlan(QB qb) throws SemanticException {

    LinkedHashMap<String, Operator> aliasToOpInfo = new LinkedHashMap<String, Operator>();

    for (String alias : qb.getSubqAliases()) {
      if (!this.hasWith) {
        QBExpr qbexpr = qb.getSubqForAlias(alias);
        aliasToOpInfo.put(alias, genPlan(qbexpr));
        qbexpr.setAlias(alias);
      } else {
        if (this.hasWith && !this.withQueries.contains(alias)) {
          QBExpr qbexpr = qb.getSubqForAlias(alias);
          aliasToOpInfo.put(alias, genPlan(qbexpr));
          qbexpr.setAlias(alias);
        } else {
          int idx = this.withQueries.indexOf(alias);
          if (!this.beProcessed.get(idx)) {
            QBExpr qbexpr = qb.getSubqForAlias(alias);
            Operator op = genPlan(qbexpr);
            aliasToOpInfo.put(alias, op);
            qbexpr.setAlias(alias);
            this.beProcessed.set(idx, true);
          } else {
            aliasToOpInfo.put(alias, this.withOps.get(alias));
          }
        }
      }
    }

    for (String alias : qb.getTabAliases()) {
      aliasToOpInfo.put(alias, genTablePlan(alias, qb));
      LOG.debug("add to aliasToOpInfo: " + alias);
    }

    for (String alias : qb.getInsertTmpTabAliases()) {
      aliasToOpInfo.put(alias, genTablePlan(alias, qb));
    }

    genLateralViewPlans(aliasToOpInfo, qb);
    Operator srcOpInfo = null;

    if (qb.getParseInfo().getJoinExpr() != null) {
      ASTNode joinExpr = qb.getParseInfo().getJoinExpr();
      if (joinExpr.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
        QBJoinTree joinTree = genUniqueJoinTree(qb, joinExpr);
        qb.setQbJoinTree(joinTree);
      } else {
        QBJoinTree joinTree = genJoinTree(qb, joinExpr);
        qb.setQbJoinTree(joinTree);
        mergeJoinTree(qb);
      }

      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    } else {

      Iterator it = aliasToOpInfo.values().iterator();
      if (it.hasNext()) {
        srcOpInfo = aliasToOpInfo.values().iterator().next();
      } else {
        throw new SemanticException(ErrorMsg.SELECT_WITHNOFROM_INSUBQ.getMsg());
      }

    }

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo);
    LOG.debug("Created Plan for Query Block " + qb.getId());

    if (this.hasWith && this.withQueries.contains(qb.getId())
        && (this.withOps.get(qb.getId()) == null)) {

      this.withOps.put(qb.getId(), bodyOpInfo);
    }

    this.qb = qb;
    return bodyOpInfo;
  }

  void genLateralViewPlans(HashMap<String, Operator> aliasToOpInfo, QB qb)
      throws SemanticException {
    Map<String, ArrayList<ASTNode>> aliasToLateralViews = qb.getParseInfo()
        .getAliasToLateralViews();
    for (Entry<String, Operator> e : aliasToOpInfo.entrySet()) {
      String alias = e.getKey();
      ArrayList<ASTNode> lateralViews = aliasToLateralViews.get(alias);
      if (lateralViews != null) {
        Operator op = e.getValue();

        for (ASTNode lateralViewTree : aliasToLateralViews.get(alias)) {

          RowResolver lvForwardRR = opParseCtx.get(op).getRR();
          Operator lvForward = putOpInsertMap(OperatorFactory.getAndMakeChild(
              new LateralViewForwardDesc(),
              new RowSchema(lvForwardRR.getColumnInfos()), op), lvForwardRR);

          RowResolver allPathRR = opParseCtx.get(lvForward).getRR();
          Operator allPath = putOpInsertMap(OperatorFactory.getAndMakeChild(
              new selectDesc(true), new RowSchema(allPathRR.getColumnInfos()),
              lvForward), allPathRR);

          QB blankQb = new QB(null, null, false);
          Operator udtfPath = genSelectPlan(null,
              (ASTNode) lateralViewTree.getChild(0), blankQb, lvForward);
          RowResolver udtfPathRR = opParseCtx.get(udtfPath).getRR();

          RowResolver lateralViewRR = new RowResolver();
          ArrayList<String> outputInternalColNames = new ArrayList<String>();

          LVmergeRowResolvers(allPathRR, lateralViewRR, outputInternalColNames);
          LVmergeRowResolvers(udtfPathRR, lateralViewRR, outputInternalColNames);

          Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

          int i = 0;
          for (ColumnInfo c : allPathRR.getColumnInfos()) {
            String internalName = getColumnInternalName(i);
            i++;
            colExprMap.put(
                internalName,
                new exprNodeColumnDesc(c.getType(), c.getInternalName(), c
                    .getTabAlias(), c.getIsPartitionCol()));
          }

          Operator lateralViewJoin = putOpInsertMap(
              OperatorFactory.getAndMakeChild(new lateralViewJoinDesc(
                  outputInternalColNames),
                  new RowSchema(lateralViewRR.getColumnInfos()), allPath,
                  udtfPath), lateralViewRR);

          lateralViewJoin.setColumnExprMap(colExprMap);
          op = lateralViewJoin;
        }
        e.setValue(op);
      }
    }
  }

  private void LVmergeRowResolvers(RowResolver source, RowResolver dest,
      ArrayList<String> outputInternalColNames) {
    Vector<ColumnInfo> cols = source.getColumnInfos();
    for (ColumnInfo c : cols) {
      String internalName = getColumnInternalName(outputInternalColNames.size());
      outputInternalColNames.add(internalName);
      ColumnInfo newCol = new ColumnInfo(internalName, c.getType(),
          c.getTabAlias(), c.getIsPartitionCol());
      String[] tableCol = source.reverseLookup(c.getInternalName());
      String tableAlias = tableCol[0];
      String colAlias = tableCol[1];
      dest.put(tableAlias, colAlias, newCol);
    }
  }

  private Operator<? extends Serializable> getReduceSink(
      Operator<? extends Serializable> top) {
    if (top.getClass() == ReduceSinkOperator.class) {
      assert (top.getChildOperators().size() == 1);

      return top;
    }

    List<Operator<? extends Serializable>> childOps = top.getChildOperators();
    if (childOps == null) {
      return null;
    }

    for (int i = 0; i < childOps.size(); ++i) {
      Operator<? extends Serializable> reducer = getReduceSink(childOps.get(i));
      if (reducer != null) {
        return reducer;
      }
    }

    return null;
  }

  @SuppressWarnings("nls")
  private void genMapRedTasks(QB qb) throws SemanticException {
    fetchWork fetch = null;
    List<Task<? extends Serializable>> mvTask = new ArrayList<Task<? extends Serializable>>();
    Task<? extends Serializable> fetchTask = null;

    int optimizeLevel = conf
        .getInt("hive.semantic.analyzer.optimizer.level", 2);

    QBParseInfo qbParseInfo = qb.getParseInfo();
    boolean large = conf.getBoolean("tdw.ide.data.export.data.large", false);
    int fetchlimit = conf.getInt("select.max.limit", -1);
    if (large) {
      fetchlimit = conf.getInt("tdw.ide.data.export.records", 2000000);
    }
    if (qbParseInfo.getOuterQueryLimit() > 0) {
      if (fetchlimit == -1) {
        fetchlimit = qbParseInfo.getOuterQueryLimit();
      } else if (fetchlimit > 0
          && qbParseInfo.getOuterQueryLimit() < fetchlimit)
        fetchlimit = qbParseInfo.getOuterQueryLimit();

    }

    if (isAllDBExternalTable) {
      String cols = loadFileWork.get(0).getColumns();
      String colTypes = loadFileWork.get(0).getColumnTypes();
      LOG.debug("cols: " + cols);
      LOG.debug("colTypes: " + colTypes);

      RowResolver inputRR = opParseCtx.get(filesinkop).getRR();
      LOG.debug(inputRR.toString());
      HashMap<String, LinkedHashMap<String, ColumnInfo>> columnMap = inputRR
          .rslvMap();
      int index = 0;
      String colNameList = new String("");
      String[] colNameType;
      Map<String, String> colMap = new HashMap<String, String>();

      Vector<ColumnInfo> colsinfo = inputRR.getColumnInfos();
      String newcolNameList = new String("");
      if (colsinfo.size() > 1) {
        newcolNameList = colsinfo.get(0).getAlias();
        for (int i = 1; i < colsinfo.size(); i++) {
          newcolNameList += ",";
          newcolNameList += colsinfo.get(i).getAlias();
        }
        LOG.debug("newcolNameList:  " + newcolNameList);
      } else if (colsinfo.size() == 1) {
        newcolNameList = colsinfo.get(0).getAlias();
        LOG.debug("newcolNameList:  " + newcolNameList);
      }

      fetch = new fetchWork(pgTmpDir, new tableDesc(LazySimpleSerDe.class,
          MyTextInputFormat.class, IgnoreKeyTextOutputFormat.class,
          Utilities.makeProperties(
              org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                  + Utilities.ctrlaCode,
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
              newcolNameList,
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
              colTypes)), fetchlimit);

      if (fetch != null) {
        fetchTask = TaskFactory.get(fetch, this.conf);
        setFetchTask(fetchTask);
        return;
      }
    }

    if (qb.isSelectStarQuery()
        && qbParseInfo.getDestToClusterBy().isEmpty()
        && qbParseInfo.getDestToDistributeBy().isEmpty()
        && qbParseInfo.getDestToOrderBy().isEmpty()
        && qbParseInfo.getDestToSortBy().isEmpty()
        && !DBExternalTableUtil.isDBExternalTable(qb, db, allTableMap,
            optimizeLevel)) {
      Iterator<Map.Entry<String, TablePartition>> iter = qb.getMetaData()
          .getAliasToTable().entrySet().iterator();
      Map.Entry<String, TablePartition> entry = iter.next();
      TablePartition tab = entry.getValue();
      if (!tab.isPartitioned()) {
        if (qbParseInfo.getDestToWhereExpr().isEmpty() 
            && !TableUtil.isTableFormatChanged(qb, db, allTableMap, optimizeLevel))
          fetch = new fetchWork(fetchWork.convertPathToStringArray(tab
              .getPaths()), Utilities.getTableDescFetch(tab.getTbl()),
              fetchlimit);
        inputs.add(new ReadEntity(tab.getTbl()));
      } else {
        if (/* aliasToPruner.size() == 0 */qbParseInfo.getDestToWhereExpr()
            .isEmpty()
            && aliasToPruner.get(entry.getKey()).getPrunerExpr() == null
            && !TableUtil.isTableFormatChanged(qb, db, allTableMap, optimizeLevel)) {

          List<String> listP = new ArrayList<String>();

          try {
            tableDesc td = Utilities.getTableDescFetch(tab.getTbl());

            for (Path path : tab.getPaths()) {
              listP.add(path.toString());
              LOG.debug("add path :" + path.toString());

            }

            indexQueryInfo.partList = listP;

            fetch = new fetchWork(listP, td, fetchlimit);
          } catch (Exception e) {
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            if (SessionState.get() != null)
              SessionState.get().ssLog(
                  org.apache.hadoop.util.StringUtils.stringifyException(e));
            throw new SemanticException(e.getMessage(), e);
          }

        }

      }
      if (fetch != null) {
        fetchTask = TaskFactory.get(fetch, this.conf);
        setFetchTask(fetchTask);
        return;
      }
    }

    if (qb.getIsQuery()
        && (qb.getNumSelDi() < 1)
        && qbParseInfo.getDestToClusterBy().isEmpty()
        && qbParseInfo.getDestToDistributeBy().isEmpty()
        && qbParseInfo.getDestToOrderBy().isEmpty()
        && qbParseInfo.getDestToSortBy().isEmpty()
        && qbParseInfo.isFitToOp()
        && qbParseInfo.getDestToWhereExpr().isEmpty()
        && qbParseInfo.getDestToGroupBy().isEmpty()
        && qbParseInfo.getDestToHaving().isEmpty()
        && !DBExternalTableUtil.isDBExternalTable(qb, db, allTableMap,
            optimizeLevel)
        && !TableUtil.isTableFormatChanged(qb, db, allTableMap, optimizeLevel)) {

      boolean flag = true;
      Vector<Integer> inlist = new Vector<Integer>();
      Vector<String> colsname = new Vector<String>();
      inlist.clear();
      colsname.clear();

      Iterator<Map.Entry<String, TablePartition>> iter = qb.getMetaData()
          .getAliasToTable().entrySet().iterator();
      if (iter.hasNext()) {
        Map.Entry<String, TablePartition> entry = iter.next();
        TablePartition tab = entry.getValue();
        List<FieldSchema> tmpList = tab.getTbl().getCols();
        Iterator itt = tmpList.iterator();
        colsname.clear();
        while (itt.hasNext()) {
          String tmpStr = ((FieldSchema) itt.next()).getName();
          colsname.add(tmpStr);
        }

        if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
          throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());

        indexQueryInfo.limit = qb.getParseInfo().getOuterQueryLimit();
        int limit_val = HiveConf.getIntVar(conf, HiveConf.ConfVars.LIMITOP);

        LOG.info("max limit: " + limit_val + "real limit: " + indexQueryInfo.limit);
        if (SessionState.get() != null) {
          SessionState.get().ssLog("max limit: " + limit_val + "real limit: " + indexQueryInfo.limit);
        }

        if (indexQueryInfo.limit > limit_val || indexQueryInfo.limit == -1) {
          flag = false;
        }

        TreeSet<String> ks = new TreeSet<String>();
        ks.addAll(qbParseInfo.getClauseNames());
        for (String dest : ks) {
          ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);
          for (int i = 0; i < selExprList.getChildCount(); i++) {
            if (selExprList.getChild(i).getType() != HiveParser.TOK_SELEXPR) {
              flag = false;
            } else {
              if (selExprList.getChild(i).getChild(0).getType() != HiveParser.TOK_TABLE_OR_COL) {
                flag = false;
              } else {
                String sss = selExprList.getChild(i).getChild(0).getChild(0)
                    .getText();
                for (int lu = 0; lu < colsname.size(); lu++) {
                  if (sss.compareToIgnoreCase(colsname.get(lu)) == 0) {
                    inlist.add(new Integer(lu));
                  }
                }
              }
            }
          }
        }

        if (flag) {
          if (!tab.isPartitioned()) {

            tableDesc tmpb = Utilities.getTableDescFetch(tab.getTbl());
            tmpb.setSelectOpInfo(inlist, tmpList, tmpList.size());

            fetch = new fetchWork(fetchWork.convertPathToStringArray(tab
                .getPaths()), tmpb, fetchlimit);

            inputs.add(new ReadEntity(tab.getTbl()));
            if (fetch != null) {
              fetchTask = TaskFactory.get(fetch, this.conf);
              setFetchTask(fetchTask);

              return;
            }
          } else {
            if (aliasToPruner.get(entry.getKey()).getPrunerExpr() == null) {
              try {
                List<String> listP = new ArrayList<String>();

                for (Path path : tab.getPaths()) {
                  listP.add(path.toString());
                  LOG.debug("add path :" + path.toString());
                }

                indexQueryInfo.partList = listP;

                tableDesc tmpb = Utilities.getTableDescFetch(tab.getTbl());
                tmpb.setSelectOpInfo(inlist, tmpList, tmpList.size());
                fetch = new fetchWork(listP, tmpb, fetchlimit);

                inputs.add(new ReadEntity(tab.getTbl()));
                if (fetch != null) {
                  fetchTask = TaskFactory.get(fetch, this.conf);
                  setFetchTask(fetchTask);
                  return;
                }
              } catch (Exception e) {
                LOG.error(org.apache.hadoop.util.StringUtils
                    .stringifyException(e));
                if (SessionState.get() != null)
                  SessionState.get().ssLog(
                      org.apache.hadoop.util.StringUtils.stringifyException(e));
                throw new SemanticException(e.getMessage(), e);
              }
            }
          }
        }

      }

    }

    if (qb.getIsQuery()) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      String cols = loadFileWork.get(0).getColumns();
      String colTypes = loadFileWork.get(0).getColumnTypes();

      RowResolver inputRR = opParseCtx.get(filesinkop).getRR();
      LOG.debug(inputRR.toString());
      HashMap<String, LinkedHashMap<String, ColumnInfo>> columnMap = inputRR
          .rslvMap();
      int index = 0;
      String colNameList = new String("");
      String[] colNameType;
      Map<String, String> colMap = new HashMap<String, String>();

      Vector<ColumnInfo> colsinfo = inputRR.getColumnInfos();
      String newcolNameList = new String("");
      if (colsinfo.size() > 1) {
        newcolNameList = colsinfo.get(0).getAlias();
        for (int i = 1; i < colsinfo.size(); i++) {
          newcolNameList += ",";
          newcolNameList += colsinfo.get(i).getAlias();
        }
        LOG.debug("newcolNameList:  " + newcolNameList);
      } else if (colsinfo.size() == 1) {
        newcolNameList = colsinfo.get(0).getAlias();
        LOG.debug("newcolNameList:  " + newcolNameList);
      }

      for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : columnMap
          .entrySet()) {
        String tab = (String) e.getKey();
        HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>) e
            .getValue();
        if (f_map != null) {
          for (Map.Entry<String, ColumnInfo> entry : f_map.entrySet()) {
            colNameType = entry.getValue().toString().split(":");
            colMap.put(colNameType[0], (String) entry.getKey());
            LOG.debug(entry.getValue().toString());
            LOG.debug((String) entry.getKey());
          }
        }
      }
      int colNum = colMap.size();
      String keyStr = new String("_col");

      for (int i = 0; i < colNum; i++) {
        if (i > 0)
          colNameList += ",";
        colNameList += colMap.get(keyStr + i);

      }
      LOG.debug(colNameList);

      fetch = new fetchWork(
          new Path(loadFileWork.get(0).getSourceDir()).toString(),
          new tableDesc(LazySimpleSerDe.class, MyTextInputFormat.class,
              IgnoreKeyTextOutputFormat.class, Utilities.makeProperties(
                  org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
                  "" + Utilities.ctrlaCode,
                  org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
                  newcolNameList,
                  org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
                  colTypes)), fetchlimit);

      fetchTask = TaskFactory.get(fetch, this.conf);
      setFetchTask(fetchTask);

      indexQueryInfo.limit = fetchlimit;
    } else {
      List<moveWork> mv = new ArrayList<moveWork>();
      for (loadTableDesc ltd : loadTableWork)
        mvTask.add(TaskFactory.get(new moveWork(ltd, null, false), this.conf));

      boolean oneLoadFile = true;
      for (loadFileDesc lfd : loadFileWork) {
        if (qb.isCTAS()) {
          assert (oneLoadFile);
          String location = qb.getTableDesc().getLocation();

          String db_name = SessionState.get().getDbName();

          if (location == null) {

            try {
              Warehouse wh = new Warehouse(conf);
              location = wh.getDefaultTablePath(db_name,
                  qb.getTableDesc().getTableName()).toString();
              LOG.debug("CTAS location:" + location);
            } catch (MetaException e) {
              throw new SemanticException(e.getMessage(), e);
            }
          }
          lfd.setTargetDir(location);
          oneLoadFile = false;
        }
        mvTask.add(TaskFactory.get(new moveWork(null, lfd, false), this.conf));
      }
    }

    if (qb.getIsQuery() && indexQueryInfo.isIndexMode) {

      Path resPath = new Path(loadFileWork.get(0).getSourceDir());

      indexWork idxWork = new indexWork(resPath, indexQueryInfo,
          Utilities.makeProperties(Constants.SERIALIZATION_FORMAT, ""
              + Utilities.ctrlaCode, Constants.LIST_COLUMNS, "",
              Constants.LIST_COLUMN_TYPES, ""));
      Task<? extends Serializable> indexTask = TaskFactory.get(idxWork,
          this.conf);
      getRootTasks().add(indexTask);

      return;
    }

    GenMRProcContext procCtx = new GenMRProcContext(
        conf,
        new HashMap<Operator<? extends Serializable>, Task<? extends Serializable>>(),
        new ArrayList<Operator<? extends Serializable>>(), getParseContext(),
        mvTask, this.rootTasks,
        new LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx>(),
        inputs, outputs);
    procCtx.setMultiHdfsInfo(multiHdfsInfo);
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R1"), "TS%"), new GenMRTableScan1());
    opRules.put(new RuleRegExp(new String("R2"), "TS%.*RS%"),
        new GenMRRedSink1());
    opRules.put(new RuleRegExp(new String("R3"), "RS%.*RS%"),
        new GenMRRedSink2());
    opRules.put(new RuleRegExp(new String("R4"), "FS%"), new GenMRFileSink1());
    opRules.put(new RuleRegExp(new String("R5"), "UNION%"), new GenMRUnion1());
    opRules.put(new RuleRegExp(new String("R6"), "UNION%.*RS%"),
        new GenMRRedSink3());
    opRules.put(new RuleRegExp(new String("R6"), "MAPJOIN%.*RS%"),
        new GenMRRedSink4());
    opRules.put(new RuleRegExp(new String("R7"), "TS%.*MAPJOIN%"),
        MapJoinFactory.getTableScanMapJoin());
    opRules.put(new RuleRegExp(new String("R8"), "RS%.*MAPJOIN%"),
        MapJoinFactory.getReduceSinkMapJoin());
    opRules.put(new RuleRegExp(new String("R9"), "UNION%.*MAPJOIN%"),
        MapJoinFactory.getUnionMapJoin());
    opRules.put(new RuleRegExp(new String("R10"), "MAPJOIN%.*MAPJOIN%"),
        MapJoinFactory.getMapJoinMapJoin());
    opRules.put(new RuleRegExp(new String("R11"), "MAPJOIN%SEL%"),
        MapJoinFactory.getMapJoin());

    Dispatcher disp = new DefaultRuleDispatcher(new GenMROperator(), opRules,
        procCtx);

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(this.topOps.values());
    ogw.startWalking(topNodes, null);

    for (Task<? extends Serializable> rootTask : rootTasks)
      breakTaskTree(rootTask);

    for (Task<? extends Serializable> rootTask : rootTasks)
      setKeyDescTaskTree(rootTask);

    PhysicalContext physicalContext = new PhysicalContext(conf,
        getParseContext(), ctx, rootTasks, fetchTask);
    physicalContext.setMultiHdfsInfo(multiHdfsInfo);
    PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer(
        physicalContext, conf);
    physicalOptimizer.optimize();

    if (qb.isCTAS()) {
      createTableDesc crtTblDesc = qb.getTableDesc();

      validateCreateTable(crtTblDesc);

      getOutputs().clear();

      Task<? extends Serializable> crtTblTask = TaskFactory.get(new DDLWork(
          getInputs(), getOutputs(), crtTblDesc), this.conf);

      HashSet<Task<? extends Serializable>> leaves = new HashSet<Task<? extends Serializable>>();
      getLeafTasks(rootTasks, leaves);
      assert (leaves.size() > 0);
      for (Task<? extends Serializable> task : leaves) {
        task.addDependentTask(crtTblTask);
      }
    }
  }

  private void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
      HashSet<Task<? extends Serializable>> leaves) {

    for (Task<? extends Serializable> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<? extends Serializable> task,
      HashSet<Task<? extends Serializable>> leaves) {
    if (task.getChildTasks() == null) {
      if (!leaves.contains(task)) {
        leaves.add(task);
      }
    } else {
      getLeafTasks(task.getChildTasks(), leaves);
    }
  }

  private void breakTaskTree(Task<? extends Serializable> task) {

    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      HashMap<String, Operator<? extends Serializable>> opMap = ((mapredWork) task
          .getWork()).getAliasToWork();
      if (!opMap.isEmpty())
        for (Operator<? extends Serializable> op : opMap.values()) {
          breakOperatorTree(op);
        }
    }

    if (task.getChildTasks() == null)
      return;

    for (Task<? extends Serializable> childTask : task.getChildTasks())
      breakTaskTree(childTask);
  }

  private void breakOperatorTree(Operator<? extends Serializable> topOp) {
    if (topOp instanceof ReduceSinkOperator)
      topOp.setChildOperators(null);

    if (topOp.getChildOperators() == null)
      return;

    for (Operator<? extends Serializable> op : topOp.getChildOperators())
      breakOperatorTree(op);
  }

  private void setKeyDescTaskTree(Task<? extends Serializable> task) {

    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      mapredWork work = (mapredWork) task.getWork();
      HashMap<String, Operator<? extends Serializable>> opMap = work
          .getAliasToWork();
      if (!opMap.isEmpty())
        for (Operator<? extends Serializable> op : opMap.values())
          GenMapRedUtils.setKeyAndValueDesc(work, op);
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks)
        setKeyDescTaskTree(tsk);
    }

    if (task.getChildTasks() == null)
      return;

    for (Task<? extends Serializable> childTask : task.getChildTasks())
      setKeyDescTaskTree(childTask);
  }

  @SuppressWarnings("nls")
  public Phase1Ctx initPhase1Ctx() {

    Phase1Ctx ctx_1 = new Phase1Ctx();
    ctx_1.nextNum = 0;
    ctx_1.dest = "reduce";

    return ctx_1;
  }

  private void mergeNode(Operator<? extends Serializable> root,
      Operator<? extends Serializable> pre, Operator<? extends Serializable> cur) {
    List<Operator<? extends Serializable>> childlistCur = (List<Operator<? extends Serializable>>) cur
        .getChildOperators();
    List<Operator<? extends Serializable>> childlistPre = (List<Operator<? extends Serializable>>) pre
        .getChildOperators();
    Iterator itCur = childlistCur.iterator();
    Iterator itPre = childlistPre.iterator();
    Operator<? extends Serializable> first;
    if (itPre.hasNext()) {
      first = (Operator<? extends Serializable>) itPre.next();
      int ind = 0;
      while (itCur.hasNext()) {
        Operator<? extends Serializable> curtmp = (Operator<? extends Serializable>) itCur
            .next();
        curtmp.setParentOperators(first.getParentOperators());
        childlistPre.add(curtmp);
      }
      childlistCur.clear();
      pre.setChildOperators(childlistPre);
      cur.setChildOperators(childlistCur);
    }
  }

  private boolean compareFilterDesc(filterDesc fir, filterDesc sec) {
    exprNodeDesc ndfir = fir.getPredicate();
    exprNodeDesc ndsec = sec.getPredicate();
    if (ndfir.getExprString().equals(ndsec.getExprString())) {
      return true;
    } else {
      return false;

    }
  }

  private boolean isSameToChild(Operator<? extends Serializable> far) {
    List<Operator<? extends Serializable>> listchild = (List<Operator<? extends Serializable>>) far
        .getChildOperators();
    if ((listchild.size() == 1)) {
      Operator<? extends Serializable> child = (Operator<? extends Serializable>) listchild
          .get(0);
      if ((child.getConf() instanceof filterDesc)
          && compareFilterDesc((filterDesc) far.getConf(),
              (filterDesc) child.getConf())) {
        List<Operator<? extends Serializable>> listchildchild = (List<Operator<? extends Serializable>>) child
            .getChildOperators();
        Iterator it = listchildchild.iterator();
        while (it.hasNext()) {
          Operator<? extends Serializable> curtmp = (Operator<? extends Serializable>) it
              .next();

          List<Operator<? extends Serializable>> tmpParentOperators = new ArrayList<Operator<? extends Serializable>>();
          for (Operator<? extends Serializable> tmpParentOp : curtmp
              .getParentOperators()) {
            if (tmpParentOp.getIdentifier() != child.getIdentifier())
              tmpParentOperators.add(tmpParentOp);
            else
              tmpParentOperators.addAll(child.getParentOperators());
          }
          curtmp.setParentOperators(tmpParentOperators);
        }
        far.setChildOperators(listchildchild);
        return true;
      }
    }
    return false;
  }

  private void subPrintOfopOps(Operator<? extends Serializable> tmp) {
    LOG.debug("Name:  " + tmp.getName());
    LOG.debug("ID:  " + tmp.getIdentifier());
    if (tmp.getChildOperators() != null) {
      List<Operator<? extends Serializable>> lll = (List<Operator<? extends Serializable>>) tmp
          .getChildOperators();
      int count = lll.size();
      LOG.debug(tmp.getIdentifier() + " has " + count + " children.");
      if (2 == count) {
        Operator<? extends Serializable> pre = (Operator<? extends Serializable>) lll
            .get(0);
        Operator<? extends Serializable> cur = (Operator<? extends Serializable>) lll
            .get(1);
        if ((pre.getConf() instanceof filterDesc)
            && (cur.getConf() instanceof filterDesc)) {
          filterDesc fdpre = (filterDesc) pre.getConf();
          filterDesc fdcur = (filterDesc) cur.getConf();
          exprNodeDesc ndpre = fdpre.getPredicate();
          exprNodeDesc ndcur = fdcur.getPredicate();
          LOG.debug(pre.getIdentifier() + " : " + ndpre.getExprString() + "  "
              + ndpre.getTypeString() + "  " + ndpre.getName());
          LOG.debug(cur.getIdentifier() + " : " + ndcur.getExprString() + "  "
              + ndpre.getTypeString() + "  " + ndpre.getName());
          if (compareFilterDesc(fdpre, fdcur)) {
            LOG.debug(pre.getIdentifier() + " and " + cur.getIdentifier()
                + " need adjustment");
            mergeNode(tmp, pre, cur);
            LOG.debug(cur.getIdentifier() + " be removed!");
            if (SessionState.get() != null)
              SessionState.get().ssLog(cur.getIdentifier() + " be removed!");
            lll.remove(cur);
            tmp.setChildOperators(lll);
          }

        }

      } else if (2 < count) {
        List<Operator<? extends Serializable>> waitList = new ArrayList<Operator<? extends Serializable>>();
        waitList.clear();
        for (int i = 0; i < count - 1; i++) {
          for (int j = i + 1; j < count; j++) {
            Operator<? extends Serializable> pre = (Operator<? extends Serializable>) lll
                .get(i);
            Operator<? extends Serializable> cur = (Operator<? extends Serializable>) lll
                .get(j);
            if ((pre.getConf() instanceof filterDesc)
                && (cur.getConf() instanceof filterDesc)) {
              filterDesc fdpre = (filterDesc) pre.getConf();
              filterDesc fdcur = (filterDesc) cur.getConf();
              if (compareFilterDesc(fdpre, fdcur)) {
                LOG.debug(pre.getIdentifier() + " and " + cur.getIdentifier()
                    + " need adjustment");
                if (!waitList.contains(cur)) {
                  mergeNode(tmp, pre, cur);
                  waitList.add(cur);
                }
              }
            }

          }
        }
        Iterator litint = waitList.iterator();
        while (litint.hasNext()) {
          Operator<? extends Serializable> ttt = (Operator<? extends Serializable>) litint
              .next();
          if (lll.contains(ttt)) {
            lll.remove(ttt);
            LOG.debug(ttt.getIdentifier() + " be removed");
            if (SessionState.get() != null)
              SessionState.get().ssLog(ttt.getIdentifier() + " be removed");
          }
        }
        tmp.setChildOperators(lll);
      } else if (1 == count) {
        Operator<? extends Serializable> curr = (Operator<? extends Serializable>) lll
            .get(0);
        if ((curr.getConf() instanceof filterDesc)) {
          filterDesc fdcurr = (filterDesc) curr.getConf();
          exprNodeDesc ndcurr = fdcurr.getPredicate();
          LOG.debug(curr.getIdentifier() + " : " + ndcurr.getExprString());
          if (this.isSameToChild(curr)) {
            LOG.info("useless filter is removed: " + curr.getIdentifier() + " : " + ndcurr.getExprString());
            if (SessionState.get() != null)
              SessionState.get().ssLog("useless filter is removed: " + curr.getIdentifier() + " : " + ndcurr.getExprString());
          }
        }

      } else {
        return;
      }
      Iterator it = lll.iterator();
      while (it.hasNext()) {
        Operator<? extends Serializable> subtmp = (Operator<? extends Serializable>) it
            .next();
        subPrintOfopOps(subtmp);
      }
    } else {

      LOG.debug(tmp.getIdentifier() + " has no child.");
    }
  }

  private void printTreeOftopOps() {
    Iterator lit = topOps.entrySet().iterator();
    while (lit.hasNext()) {
      Map.Entry e = (Map.Entry) lit.next();
      LOG.debug("Key: " + e.getKey());
      Operator<? extends Serializable> tmp_op = (Operator<? extends Serializable>) e
          .getValue();
      this.subPrintOfopOps(tmp_op);
    }
  }

  public void changeASTTreeForIpInfoFuncTableNameToPath(ASTNode ast1)
      throws SemanticException {
    ASTNode ast2 = (ASTNode) ast1.getChild(1);
    if (ast2.getType() == HiveParser.StringLiteral) {
      LOG.debug("before ipinfo:changeTableNametoPath::" + ast1.toStringTree());
      ASTNode ast_number = (ASTNode) ast1.getChild(3);
      if (ast_number.getType() == HiveParser.TOK_NULL) {
        throw new SemanticException("udf:ipinfo:table_index: "
            + ast_number.toString() + " cannot be null.");
      }
      String tableName = BaseSemanticAnalyzer.unescapeSQLString(ast2.getText());
      String dbName = "default_db";
      String tablePath;
      if (ipinfoTables.containsKey(tableName)) {
        tablePath = ipinfoTables.get(tableName);
      } else {
        int index = tableName.indexOf("::");
        if (index > 0) {
          dbName = tableName.substring(0, index);
          tableName = tableName.substring(index + 2);
        }
        try {
          this.db.getDatabase(dbName);
        } catch (Exception e) {
          throw new SemanticException("get   database : " + dbName
              + " error,make sure it exists!");
        }
        try {
          if (!db.hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.SELECT_PRIV, dbName, tableName)) {
            throw new SemanticException("user : "
                + SessionState.get().getUserName()
                + " do not have SELECT privilege on table : " + dbName + "::"
                + tableName);
          }
          Table tab = this.db.getTable(dbName, tableName);
          tablePath = tab.getPath().toString();
          Class<? extends InputFormat> inputFormat = tab.getInputFormatClass();
          if (inputFormat != TextInputFormat.class) {
            throw new SemanticException("udf:ipinfo " + tableName
                + " should be a text table ");
          }
          if (tab.isPartitioned()) {
            throw new SemanticException("udf:ipinfo " + tableName
                + " should be a no-partition table ");
          }
          if (tab.isCompressed()) {
            throw new SemanticException("udf:ipinfo " + tableName
                + " should be a no-compressed table ");
          }
          List<FieldSchema> fields = tab.getTTable().getSd().getCols();
          if (fields.size() < 2) {
            throw new SemanticException("udf:ipinfo " + tableName
                + " column number should >= 2 ");
          }
          for (int k = 0; k < fields.size(); k++) {
            tablePath += "," + fields.get(k).getType();
            if (k == 0 || k == 1) {
              String typeName = fields.get(k).getType();
              if (!typeName.equalsIgnoreCase("bigint")) {
                throw new SemanticException("udf:ipinfo " + tableName
                    + "(start_ip/end_ip) column 0 or 1 type is not bigint");
              }
            }
          }
          ipinfoTables.put(tableName, tablePath);
        } catch (SemanticException se) {
          throw se;
        } catch (HiveException e) {
          e.printStackTrace();
          throw new SemanticException("udf:ipinfo " + tableName + " not found");
        }
      }
      ipinfoExist = true;
      ast1.setChild(1,
          ASTNode.get(HiveParser.StringLiteral, "'" + tablePath + "'"));
      LOG.debug("after ipinfo:changeTableNametoPath::" + ast1.toStringTree());
    } else {
      throw new SemanticException("udf:ipinfo:tableName: " + ast2.toString()
          + " should be string type.");
    }
  }

  public void changeASTTreesForOptimizeInsertWithoutUnion(ASTNode ast_root)
      throws SemanticException {
    ASTNode ast = ast_root;
    if (ast.getToken() != null
        && ast.getToken().getType() == HiveParser.TOK_QUERY
        && ast.getChildCount() == 2) {
      ASTNode query = ast;
      ASTNode rinsert = (ASTNode) ast.getChild(1);
      ASTNode rdest = (ASTNode) rinsert.getChild(0);
      ast = (ASTNode) ast.getChild(0);
      if (ast.getToken().getType() == HiveParser.TOK_FROM) {
        ast = (ASTNode) ast.getChild(0);
        if (ast.getToken().getType() == HiveParser.TOK_SUBQUERY
            && ast.getChildCount() == 1) {
          ast = (ASTNode) ast.getChild(0);
          if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
            ASTNode subfrom = (ASTNode) ast.getChild(0);
            ast = (ASTNode) ast.getChild(1);
            if (ast.getToken().getType() == HiveParser.TOK_INSERT) {
              ASTNode subinsert = ast;
              ast = (ASTNode) ast.getChild(0).getChild(0);
              if (ast.getToken().getType() == HiveParser.TOK_DIR) {
                ast = (ASTNode) ast.getChild(0);
                if (ast.getToken().getType() == HiveParser.TOK_TMP_FILE) {
                  if (rinsert.getChildCount() == 2) {
                    ast = (ASTNode) rinsert.getChild(1);
                    if (ast.getToken().getType() == HiveParser.TOK_SELECT) {
                      ast = (ASTNode) ast.getChild(0);
                      if (ast.getToken().getType() == HiveParser.TOK_SELEXPR) {
                        ast = (ASTNode) ast.getChild(0);
                        if (ast.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
                          subinsert.setChild(0, rdest);
                          query.setChild(0, subfrom);
                          query.setChild(1, subinsert);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    } else if (ast.getToken() == null) {
      int child_count = ast.getChildCount();
      if (child_count != 0) {
        changeASTTreesForOptimizeInsertWithoutUnion((ASTNode) ast.getChild(0));
      }
    }
  }

  private PartitionDesc getPartDesc(ASTNode ast, List<FieldSchema> cols)
      throws SemanticException {
    try {
      PartitionDesc partDesc = getOneLevelPartitionDesc(
          (ASTNode) ast.getChild(0), cols);

      if (ast.getChildCount() == 2) {
        partDesc.setSubPartition(getOneLevelPartitionDesc(
            (ASTNode) ast.getChild(1), cols));

        if (partDesc.getPartitionType().equals(PartitionType.HASH_PARTITION)
            && partDesc.getSubPartition().getPartitionType()
                .equals(PartitionType.HASH_PARTITION))
          throw new SemanticException(
              "The two partitions should not be hash type simultaneously!");
        else if (partDesc.getPartitionType().equals(
            PartitionType.HASH_PARTITION)
            && !partDesc.getSubPartition().getPartitionType()
                .equals(PartitionType.HASH_PARTITION))
          throw new SemanticException(
              "Only the second partition can be hash type!");

      }
      return partDesc;
    } catch (SemanticException e) {
      throw e;

    }

  }

  private PartitionDesc getOneLevelPartitionDesc(ASTNode ast,
      List<FieldSchema> cols) throws SemanticException {
    PartitionDesc pardesc = new PartitionDesc();

    pardesc.setDbName(SessionState.get().getDbName());

    Boolean isRange = false;
    Boolean isDefault = false;
    if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("range")) {
      pardesc.setPartitionType(PartitionType.RANGE_PARTITION);
      isRange = true;
    } else if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("list")) {
      pardesc.setPartitionType(PartitionType.LIST_PARTITION);
    } else if (ast.getChild(0).getChild(0).getText()
        .equalsIgnoreCase("hashkey")) {
      pardesc.setPartitionType(PartitionType.HASH_PARTITION);
    } else
      throw new SemanticException("the partition type do not support now!");

    String partCol = unescapeIdentifier(ast.getChild(0).getChild(1).getText());
    for (int i = 0; i < cols.size(); ++i) {
      if (cols.get(i).getName().equalsIgnoreCase(partCol)) {
        pardesc.setPartColumn(cols.get(i));
        break;
      }
    }
    if (pardesc.getPartColumn() == null)
      throw new SemanticException("partition column : " + partCol
          + " do not present in the table columns");

    if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("hashkey")) {
      LinkedHashMap<String, List<String>> partitionSpaces = new LinkedHashMap<String, List<String>>();
      for (int i = 0; i < this.numOfHashPar; i++)
        if (i < 10) {
          partitionSpaces.put("Hash_" + "000" + i, null);
        } else if (i < 100)
          partitionSpaces.put("Hash_" + "00" + i, null);
        else if (i < 1000)
          partitionSpaces.put("Hash_" + "0" + i, null);
        else
          partitionSpaces.put("Hash_" + i, null);

      pardesc.setPartitionSpaces(partitionSpaces);
      return pardesc;
    }

    String type = pardesc.getPartColumn().getType();
    if (type.startsWith(Constants.MAP_TYPE_NAME)
        || type.startsWith(Constants.LIST_TYPE_NAME)
        || type.startsWith(Constants.STRUCT_TYPE_NAME)) {
      throw new SemanticException("Partition column ["
          + pardesc.getPartColumn().getName() + "] is not of a primitive type");
    }

    if (type.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
      throw new SemanticException("Partition column ["
          + pardesc.getPartColumn().getName()
          + "] is boolean type ,TDW forbid partititon by boolean type");
    }

    if (ast.getChild(1) == null) {
      throw new SemanticException(
          "Partition list is needed if you create partition table.");
    }

    PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
    pti.setTypeName(type);

    ObjectInspector StringIO = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    ObjectInspector ValueIO = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());
    ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters
        .getConverter(StringIO, ValueIO);
    ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters
        .getConverter(StringIO, ValueIO);

    LinkedHashMap<String, List<String>> partitionSpaces = new LinkedHashMap<String, List<String>>();

    int nmuCh = ast.getChild(1).getChildCount();
    String tmpPartDef;
    for (int i = 0; i < nmuCh; ++i) {
      if (((ASTNode) (ast.getChild(1).getChild(i).getChild(0))).getToken()
          .getType() == HiveParser.TOK_DEFAULTPARTITION) {
        if (!isDefault) {
          isDefault = true;
        } else
          throw new SemanticException("duplicate default partition for table");
        continue;
      }

      List<String> partDefine = new ArrayList<String>();
      String partName = unescapeIdentifier(
          ast.getChild(1).getChild(i).getChild(0).getText()).toLowerCase();

      int numDefine = ast.getChild(1).getChild(i).getChild(1).getChildCount();
      ASTNode parDefine = (ASTNode) ast.getChild(1).getChild(i).getChild(1);
      if ((parDefine.getToken().getType() == HiveParser.TOK_RANGEPARTITIONDEFINE && isRange)
          || (parDefine.getToken().getType() == HiveParser.TOK_LISTPARTITIONDEFINE && !isRange)) {

        for (int j = 0; j < numDefine; ++j) {
          if (((ASTNode) (ast.getChild(1).getChild(i).getChild(1).getChild(j)))
              .getToken().getType() == HiveParser.CharSetLiteral) {
            tmpPartDef = BaseSemanticAnalyzer.charSetString(ast.getChild(1)
                .getChild(i).getChild(1).getChild(j).getChild(0).getText(), ast
                .getChild(1).getChild(i).getChild(1).getChild(j).getChild(1)
                .getText());
          } else if (((ASTNode) (ast.getChild(1).getChild(i).getChild(1)
              .getChild(j))).getToken().getType() == HiveParser.StringLiteral) {
            tmpPartDef = unescapeSQLString(ast.getChild(1).getChild(i)
                .getChild(1).getChild(j).getText());
          } else {
            tmpPartDef = ast.getChild(1).getChild(i).getChild(1).getChild(j)
                .getText();

          }

          if (partDefine.contains(tmpPartDef))
            throw new SemanticException("in partition define: " + partName
                + " Value :" + tmpPartDef + " duplicated");

          if (converter1.convert(tmpPartDef) == null) {
            throw new SemanticException("value : " + tmpPartDef
                + " should be the type of " + pardesc.getPartColumn().getType());
          }

          partDefine.add(tmpPartDef);
        }
        if (partitionSpaces.containsKey(partName.toLowerCase())) {
          throw new SemanticException(
              "duplicate partition name for partition: " + partName);
        } else
          partitionSpaces.put(partName.toLowerCase(), partDefine);
      } else
        throw new SemanticException(
            "partition define should be same with partition type!");
    }

    List<List<String>> checkDefineList = new ArrayList<List<String>>();
    checkDefineList.addAll(partitionSpaces.values());
    if (pardesc.getPartitionType() == PartitionType.RANGE_PARTITION) {
      for (int i = 0; i + 1 < checkDefineList.size(); ++i) {
        Object o1 = converter1.convert(checkDefineList.get(i).get(0));
        Object o2 = converter2.convert(checkDefineList.get(i + 1).get(0));

        if (((Comparable) o1).compareTo((Comparable) o2) >= 0) {
          LOG.error(o1.getClass() + " : " + o1 + ", " + o2.getClass() + ":"
              + o2);
          throw new SemanticException("range less than value: "
              + checkDefineList.get(i).get(0) + " should smaller than: "
              + checkDefineList.get(i + 1).get(0));
        }
      }
    }

    if (pardesc.getPartitionType() == PartitionType.LIST_PARTITION) {
      for (int i = 0; i < checkDefineList.size() - 1; ++i) {
        for (int j = i + 1; j < checkDefineList.size(); ++j) {
          for (int current = 0; current < checkDefineList.get(i).size(); ++current) {
            if (checkDefineList.get(j).contains(
                checkDefineList.get(i).get(current)))
              throw new SemanticException("value: "
                  + checkDefineList.get(i).get(current)
                  + " found in mult List partition define!");
          }
        }
      }
    }

    if (isDefault) {
      partitionSpaces.put("default", new ArrayList<String>());
    }
    if (partitionSpaces.size() > 65536) {
      throw new SemanticException(
          "TDW do not support partititon spaces more than 65536!");
    }
    pardesc.setPartitionSpaces(partitionSpaces);
    return pardesc;

  }

  private List<FieldSchema> getColsFromJar(String msgName)
      throws SemanticException {
    LOG.debug("in getColsFromJar messageName :" + msgName);

    String modified_time = null;
    LOG.debug("dbName: " + SessionState.get().getDbName().toLowerCase());
    LOG.debug("tableName: " + msgName.toLowerCase());
    modified_time = PBJarTool.downloadjar(SessionState.get().getDbName().toLowerCase(),
        msgName.toLowerCase(), conf);

    pb_msg_outer_name = SessionState.get().getDbName().toLowerCase() + "_"
        + msgName + "_" + modified_time;
    String full_name = PBJarTool.protobufPackageName + "." + pb_msg_outer_name + "$"
        + msgName;
    Descriptor message;
    String jar_path = "./auxlib/" + pb_msg_outer_name + ".jar";
    boolean flag = SessionState.get().delete_resource(
        SessionState.ResourceType.JAR, jar_path);

    if (flag) {
      LOG.info("Deleted " + jar_path + " from class path");
    }
    flag = SessionState.get().unregisterJar(jar_path);
    if (flag) {
      LOG.info("clear  " + jar_path + " from class path");
    }
    SessionState.get().add_resource(SessionState.ResourceType.JAR, jar_path);

    try {
      Class<?> outer = Class.forName(full_name, true,
          JavaUtils.getpbClassLoader());
      Method m = outer.getMethod("getDescriptor", (Class<?>[]) null);
      message = (Descriptor) m.invoke(null, (Object[]) null);
    } catch (ClassNotFoundException e) {
      throw new SemanticException("Can't find Class: " + full_name
          + " check if " + pb_msg_outer_name + ".jar is created by makejar");
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }
    List<FieldSchema> colList = new ArrayList<FieldSchema>();

    List<FieldDescriptor> fields = message.getFields();
    for (FieldDescriptor field : fields) {
      FieldSchema col = new FieldSchema();
      String name = field.getName();
      col.setName(name);
      String type = getTypeStringFromfield(field, true);
      col.setType(type);
      colList.add(col);
    }
    return colList;

  }

  private String getTypeStringFromfield(FieldDescriptor field,
      boolean checkrepeat) {
    String typeStr;
    if (checkrepeat && field.isRepeated()) {
      typeStr = Constants.LIST_TYPE_NAME + "<"
          + getTypeStringFromfield(field, false) + ">";
      return typeStr;
    }

    if (field.getType() == FieldDescriptor.Type.MESSAGE
        || field.getType() == FieldDescriptor.Type.GROUP) {
      typeStr = getStructTypeStringFromMsg(field.getMessageType());
    } else {
      typeStr = getHivePrimitiveTypeFromPBPrimitiveType(field.getType()
          .toString());
    }

    return typeStr;
  }

  private String getStructTypeStringFromMsg(Descriptor msg) {
    String typeStr = Constants.STRUCT_TYPE_NAME + "<";
    List<FieldDescriptor> fields = msg.getFields();
    int children = fields.size();
    for (int i = 0; i < children; i++) {
      FieldDescriptor field = fields.get(i);
      typeStr += field.getName() + ":";
      typeStr += getTypeStringFromfield(field, true);
      if (i < children - 1) {
        typeStr += ",";
      }
    }
    typeStr += ">";
    return typeStr;
  }

  private String getHivePrimitiveTypeFromPBPrimitiveType(String type) {
    if (type.equalsIgnoreCase("double")) {
      return Constants.DOUBLE_TYPE_NAME;
    } else if (type.equalsIgnoreCase("int32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("int64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("uint32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("uint64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sint32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sint64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("fixed32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("fixed64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sfixed32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sfixed64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("bool")) {
      return Constants.BOOLEAN_TYPE_NAME;
    } else if (type.equalsIgnoreCase("string")) {
      return Constants.STRING_TYPE_NAME;
    } else if (type.equalsIgnoreCase("float")) {
      return Constants.FLOAT_TYPE_NAME;
    } else {
      LOG.error("not hive type for pb type : " + type);
      return null;
    }
  }

  private void getProjectionInfo(ASTNode child,
      ArrayList<ArrayList<String>> projectionInfos) {
    int projectionNum = child.getChildCount();
    LOG.error("column projection, projection num:" + projectionNum);

    for (int i = 0; i < projectionNum; i++) {
      ASTNode subProjectionNode = (ASTNode) child.getChild(i);

      ArrayList<String> subProjectionList = new ArrayList<String>(10);
      int projFieldNum = subProjectionNode.getChildCount();
      for (int j = 0; j < projFieldNum; j++) {
        ASTNode subProjectionFieldNode = (ASTNode) subProjectionNode
            .getChild(j);
        subProjectionList.add(unescapeIdentifier(subProjectionFieldNode
            .getText()));
      }

      projectionInfos.add(subProjectionList);
    }

    for (int i = 0; i < projectionInfos.size(); i++) {
      LOG.error("projection " + i + ":");

      String tmpString = "";
      ArrayList<String> iter = projectionInfos.get(i);
      for (int j = 0; j < iter.size(); j++) {
        tmpString += iter.get(j) + ",";
      }
      LOG.error(tmpString);
    }
  }

  private ASTNode analyzeCreateView(ASTNode ast, QB qb)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    List<FieldSchema> cols = null;
    boolean ifNotExists = false;
    boolean orReplace = false;
    String comment = null;
    ASTNode selectStmt = null;
    Map<String, String> tblProps = null;

    LOG.debug("Creating view " + tableName + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_ORREPLACE:
        orReplace = true;
        break;
      case HiveParser.TOK_QUERY:
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLNAME:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_PROPERTIESWITH:
        if (tblProps == null)
          tblProps = new HashMap<String, String>();
        for (int propChild = 0; propChild < child.getChildCount(); propChild++) {
          ASTNode prop = (ASTNode) child.getChild(propChild);
          switch (prop.getToken().getType()) {
          case HiveParser.TOK_CHARSET:
            String charset = unescapeSQLString(prop.getChild(0).getText());

            if (Charset.isSupported(charset)) {
              tblProps.put(Constants.SERIALIZATION_CHARSET, charset);
            } else
              throw new SemanticException("cannot support this charset:"
                  + charset);
            break;
          default:
            throw new SemanticException("cannot set this properties"
                + prop.toStringTree());
          }
        }
        break;
      default:
        assert false;
      }
    }

    if (ifNotExists && orReplace) {
      throw new SemanticException("Can't combine IF NOT EXISTS and OR REPLACE.");
    }

    createVwDesc = new createViewDesc(tableName, cols, comment, tblProps,
        ifNotExists, orReplace);
    unparseTranslator.enable();
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createVwDesc), conf));
    return selectStmt;
  }

  private List<String> validateColumnNameUniqueness(
      List<FieldSchema> fieldSchemas) throws SemanticException {
    Iterator<FieldSchema> iterCols = fieldSchemas.iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName))
          throw new SemanticException(
              ErrorMsg.DUPLICATE_COLUMN_NAMES.getMsg(oldColName));
      }
      colNames.add(colName);
    }
    return colNames;
  }

  private void validateCreateTable(createTableDesc crtTblDesc)
      throws SemanticException {
    if ((crtTblDesc.getCols() == null) || (crtTblDesc.getCols().size() == 0)) {
      if (StringUtils.isEmpty(crtTblDesc.getSerName())
          || !SerDeUtils.shouldGetColsFromSerDe(crtTblDesc.getSerName())) {
        throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE.getMsg());
      }
      return;
    }

    try {

      Class<?> origin = Class.forName(crtTblDesc.getOutputFormat(), true,
          JavaUtils.getClassLoader());
      LOG.info("load outputFormat class OK!");
      if (SessionState.get() != null)
        SessionState.get().ssLog("load outputFormat class OK!");

      Class<? extends HiveOutputFormat> replaced = HiveFileFormatUtils
          .getOutputFormatSubstitute(origin);
      LOG.info("load outputFormat replaced OK!");
      if (SessionState.get() != null)
        SessionState.get().ssLog("load outputFormat replaced OK!");

      if (replaced == null)
        throw new SemanticException(
            ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
    }

    List<String> colNames = validateColumnNameUniqueness(crtTblDesc.getCols());

    if (crtTblDesc.getBucketCols() != null) {
      Iterator<String> bucketCols = crtTblDesc.getBucketCols().iterator();
      while (bucketCols.hasNext()) {
        String bucketCol = bucketCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (bucketCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }

    if (crtTblDesc.getSortCols() != null) {
      Iterator<Order> sortCols = crtTblDesc.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next().getCol();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (sortCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }

    if (crtTblDesc.getProjectionInfos() != null) {
      if (crtTblDesc.getProjectionInfos().size() > 20) {
        throw new SemanticException(ErrorMsg.PROJECTION_TOO_MANY.getMsg());
      }

      ArrayList<String> allProjectionField = new ArrayList<String>(20);

      ArrayList<ArrayList<String>> projectionInfos = crtTblDesc
          .getProjectionInfos();
      for (int i = 0; i < projectionInfos.size(); i++) {
        ArrayList<String> projectField = projectionInfos.get(i);
        for (int j = 0; j < projectField.size(); j++) {
          allProjectionField.add(projectField.get(j));
        }
      }

      Iterator<String> iterProjectionCol = allProjectionField.iterator();
      List<String> tmpFieldNames = new ArrayList<String>();
      while (iterProjectionCol.hasNext()) {
        String colName = iterProjectionCol.next();
        Iterator<String> iter = tmpFieldNames.iterator();
        while (iter.hasNext()) {
          String oldColName = iter.next();
          if (colName.equalsIgnoreCase(oldColName)) {
            throw new SemanticException(ErrorMsg.PROJECTION_FIELD_DUP.getMsg());
          }
        }
        tmpFieldNames.add(colName);
      }

      Iterator<String> iter = tmpFieldNames.iterator();
      while (iter.hasNext()) {
        boolean exist = false;
        String projectionColName = iter.next();

        Iterator<FieldSchema> tblColIter = crtTblDesc.getCols().iterator();
        while (tblColIter.hasNext()) {
          String colName = tblColIter.next().getName();
          if (colName.equalsIgnoreCase(projectionColName)) {
            exist = true;
            break;
          }
        }

        if (!exist) {
          throw new SemanticException(
              ErrorMsg.PROJECTION_FIELD_NOT_EXIST.getMsg());
        }
      }
    }

    if (crtTblDesc.getTblType() == 2 && crtTblDesc.getCols().size() > 20
        && crtTblDesc.getProjectionInfos() == null) {
      throw new SemanticException(ErrorMsg.PROJECTION_NOT_DEFINE.getMsg());
    }

    if (crtTblDesc.getIndexInfo() != null) {

      Iterator<String> iterFieldCol = crtTblDesc.getIndexInfo().fieldList
          .iterator();
      List<String> tmpFieldNames = new ArrayList<String>();
      while (iterFieldCol.hasNext()) {
        String colName = iterFieldCol.next();
        Iterator<String> iter = tmpFieldNames.iterator();
        while (iter.hasNext()) {
          String oldColName = iter.next();
          if (colName.equalsIgnoreCase(oldColName)) {
            throw new SemanticException(ErrorMsg.INDEX_FIELD_DUP.getMsg());
          }
        }
        tmpFieldNames.add(colName);
      }

      Iterator<String> iter = crtTblDesc.getIndexInfo().fieldList.iterator();
      while (iter.hasNext()) {
        boolean exist = false;
        String fieldColName = iter.next();

        Iterator<FieldSchema> tblColIter = crtTblDesc.getCols().iterator();
        while (tblColIter.hasNext()) {
          String colName = tblColIter.next().getName();
          if (colName.equalsIgnoreCase(fieldColName)) {
            exist = true;
            break;
          }
        }

        if (!exist) {
          throw new SemanticException(ErrorMsg.INDEX_FIELD_NOT_EXIST.getMsg());
        }
      }
    }
  }

  public ASTNode analyzeCreateTable(ASTNode ast, QB qb)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String likeTableName = null;
    List<FieldSchema> cols = null;
    List<FieldSchema> partCols = null;
    PartitionDesc partDesc = null;
    List<String> bucketCols = null;
    List<Order> sortCols = null;
    int numBuckets = -1;
    String fieldDelim = null;
    String fieldEscape = null;
    String collItemDelim = null;
    String mapKeyDelim = null;
    String lineDelim = null;
    String comment = null;
    String inputFormat = TEXTFILE_INPUT;
    String outputFormat = TEXTFILE_OUTPUT;
    String location = null;
    String serde = null;
    Map<String, String> mapProp = null;
    Map<String, String> mapPropOther = null;
    boolean ifNotExists = false;
    boolean isExt = false;

    boolean pgTableFlag = false;

    ASTNode selectStmt = null;
    final int CREATE_TABLE = 0;
    final int CTLT = 1;
    final int CTAS = 2;
    int command_type = CREATE_TABLE;

    ArrayList<ArrayList<String>> projectionInfos = null;
    boolean compress = false;
    int tblType = -1;
    ASTNode partdef = null;

    indexInfoDesc indexInfo = null;
    String protoFileName = null;

    if ("SequenceFile".equalsIgnoreCase(conf
        .getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
      inputFormat = SEQUENCEFILE_INPUT;
      outputFormat = SEQUENCEFILE_OUTPUT;
    }

    if (SessionState.get() != null)
      SessionState.get().ssLog("Creating table : " + tableName);

    LOG.debug("Creating table " + tableName + " positin="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    boolean tabletypesetted = false;
    boolean serdesetted = false;
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.KW_EXTERNAL:
        isExt = true;
        break;
      case HiveParser.TOK_LIKETABLE:
        if (child.getChildCount() > 0) {
          likeTableName = unescapeIdentifier(child.getChild(0).getText());
          if (likeTableName != null) {
            if (command_type == CTAS) {
              throw new SemanticException(
                  ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
            }
            if (cols != null) {
              throw new SemanticException(
                  ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
            }
          }
          command_type = CTLT;
        }
        break;
      case HiveParser.TOK_QUERY:
        if (command_type == CTLT) {
          throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
        }
        if (cols != null) {
          throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
        }
        if (partDesc != null || bucketCols != null) {
          throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
        }
        if (isExt && !pgTableFlag) {
          throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
        }

        command_type = CTAS;
        if (qb.getParseInfo().getNoColsPartTree() != null)
          throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLLIST:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case /* HiveParser.TOK_TABLEPARTCOLS */HiveParser.TOK_TABLEPARTITION:
        if (cols == null) {
          qb.getParseInfo().setNoColsPartTree(child);
        } else {
          partDesc = getPartDesc((ASTNode) child, cols);

          partDesc.setTableName(tableName);
          if (partDesc.getSubPartition() != null)
            partDesc.getSubPartition().setTableName(tableName);
        }

        break;
      case HiveParser.TOK_TABLEBUCKETS:
        bucketCols = getColumnNames((ASTNode) child.getChild(0));
        if (child.getChildCount() == 2)
          numBuckets = (Integer.valueOf(child.getChild(1).getText()))
              .intValue();
        else {
          sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
          numBuckets = (Integer.valueOf(child.getChild(2).getText()))
              .intValue();
        }
        break;
      case HiveParser.TOK_TABLEROWFORMAT:

        child = (ASTNode) child.getChild(0);
        int numChildRowFormat = child.getChildCount();
        for (int numC = 0; numC < numChildRowFormat; numC++) {
          ASTNode rowChild = (ASTNode) child.getChild(numC);
          switch (rowChild.getToken().getType()) {
          case HiveParser.TOK_TABLEROWFORMATFIELD:
            fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
            if (rowChild.getChildCount() >= 2) {
              fieldEscape = unescapeSQLString(rowChild.getChild(1).getText());
            }
            break;
          case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
            collItemDelim = unescapeSQLString(rowChild.getChild(0).getText());
            break;
          case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
            mapKeyDelim = unescapeSQLString(rowChild.getChild(0).getText());
            break;
          case HiveParser.TOK_TABLEROWFORMATLINES:
            lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
            break;
          default:
            assert false;
          }
        }
        break;
      case HiveParser.TOK_TABLESERIALIZER:

        child = (ASTNode) child.getChild(0);
        serde = unescapeSQLString(child.getChild(0).getText());
        serdesetted = true;
        if (child.getChildCount() == 2) {
          if (mapProp == null)
            mapProp = new HashMap<String, String>();
          ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1)).getChild(0);
          for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
            String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
                .getText());
            String value = unescapeSQLString(prop.getChild(propChild)
                .getChild(1).getText());
            mapProp.put(key, value);
          }
        }
        break;
      case HiveParser.TOK_TBLSEQUENCEFILE:
        inputFormat = SEQUENCEFILE_INPUT;
        outputFormat = SEQUENCEFILE_OUTPUT;
        tabletypesetted = true;
        break;
      case HiveParser.TOK_TBLTEXTFILE:
        inputFormat = TEXTFILE_INPUT;
        outputFormat = TEXTFILE_OUTPUT;
        tabletypesetted = true;

        break;
      case HiveParser.TOK_TBLRCFILE:
        if (isExt) {
          throw new SemanticException(
              "rcfile storage disallowed for extended table");
        }

        int rcfileChildNum = child.getChildCount();
        child = (ASTNode) child.getChild(0);
        if (rcfileChildNum != 0
            && child.getToken().getType() == HiveParser.TOK_COMPRESS) {
          LOG.info("rcfile compress set");
          compress = true;
        } else {
          LOG.info("rcfile not compress");
        }
        inputFormat = RCFILE_INPUT;
        outputFormat = RCFILE_OUTPUT;
        tabletypesetted = true;

        serde = COLUMNAR_SERDE;
        serdesetted = true;
        tblType = 4;
        break;

      case HiveParser.TOK_PB_FILE:

        for (int i = 1; i < numCh; i++) {
          ASTNode query = (ASTNode) ast.getChild(i);
          if (query.getToken().getType() == HiveParser.TOK_QUERY) {
            throw new SemanticException(ErrorMsg.CTAS_PB_COEXISTENCE.getMsg());
          }
        }

        inputFormat = PB_INPUT;
        outputFormat = PB_OUTPUT;
        tabletypesetted = true;

        serde = PB_SERDE;
        serdesetted = true;
        protoFileName = tableName.toLowerCase() + ".proto";

        if (cols != null) {
          throw new SemanticException("PB stored table do not need columns!");
        }
        cols = getColsFromJar(tableName.toLowerCase());

        partdef = qb.getParseInfo().getNoColsPartTree();
        if (partdef != null) {
          throw new SemanticException(
              ErrorMsg.PBFILE_PARTITION_NOTSUPPORT.getMsg());
        }

        break;
      case HiveParser.TOK_PB:

        for (int i = 1; i < numCh; i++) {
          ASTNode query = (ASTNode) ast.getChild(i);
          if (query.getToken().getType() == HiveParser.TOK_QUERY) {
            throw new SemanticException(ErrorMsg.CTAS_PB_COEXISTENCE.getMsg());
          }
        }

        inputFormat = PB_INPUT1;
        outputFormat = PB_OUTPUT1;
        tabletypesetted = true;

        serde = PB_SERDE;
        serdesetted = true;
        protoFileName = tableName.toLowerCase() + ".proto";

        if (cols != null) {
          throw new SemanticException("PB stored table do not need columns!");
        }
        cols = getColsFromJar(tableName.toLowerCase());

        partdef = qb.getParseInfo().getNoColsPartTree();
        if (partdef != null) {
          partDesc = getPartDesc((ASTNode) child, cols);

          partDesc.setTableName(tableName);
          if (partDesc.getSubPartition() != null)
            partDesc.getSubPartition().setTableName(tableName);
        }

        break;
      case HiveParser.TOK_TABLEFILEFORMAT:
        inputFormat = unescapeSQLString(child.getChild(0).getText());
        outputFormat = unescapeSQLString(child.getChild(1).getText());
        tabletypesetted = true;

        break;
      case HiveParser.TOK_TABLELOCATION:

        if (!isExt) {
          throw new SemanticException(
              "Table Location disallowed for internal table");
        }

        location = unescapeSQLString(child.getChild(0).getText());
        break;

      case HiveParser.TOK_TBLFORMATFILE:
        if (isExt) {
          throw new SemanticException(
              "format storage disallowed for extended table");
        }

        tblType = 1;

        int formatChildNum = child.getChildCount();
        child = (ASTNode) child.getChild(0);
        if (formatChildNum != 0
            && child.getToken().getType() == HiveParser.TOK_COMPRESS) {
          LOG.error("compress set");
          compress = true;
        } else {
          LOG.error("not compress");
        }

        inputFormat = "StorageEngineClient.FormatStorageInputFormat";
        outputFormat = "StorageEngineClient.FormatStorageHiveOutputFormat";
        tabletypesetted = true;

        serde = "StorageEngineClient.FormatStorageSerDe";
        serdesetted = true;

        break;

      case HiveParser.TOK_TBLCOLUMNFILE:

        if (isExt) {
          throw new SemanticException(
              "column storage disallowed for extended table");
        }

        tblType = 2;

        int columnChildNum = child.getChildCount();
        if (columnChildNum == 0) {
          LOG.error("no compress, no project");
        } else if (columnChildNum == 1) {
          child = (ASTNode) child.getChild(0);
          if (child.getToken().getType() == HiveParser.TOK_COMPRESS) {
            LOG.error("column compress");
            compress = true;
          } else if (child.getToken().getType() == HiveParser.TOK_PROJECTION) {
            if (projectionInfos == null) {
              projectionInfos = new ArrayList<ArrayList<String>>(10);
            }

            getProjectionInfo(child, projectionInfos);
          } else {
            LOG.error("column, not compress or projection");
          }
        } else {
          LOG.error("child num:" + columnChildNum);
          ASTNode tmpChild = (ASTNode) child.getChild(0);
          if (tmpChild.getToken().getType() == HiveParser.TOK_COMPRESS) {
            compress = true;
            LOG.error("compress set 0");
          } else {
            if (projectionInfos == null) {
              projectionInfos = new ArrayList<ArrayList<String>>(10);
            }
            getProjectionInfo(tmpChild, projectionInfos);
          }

          tmpChild = (ASTNode) child.getChild(1);
          if (tmpChild.getToken().getType() == HiveParser.TOK_COMPRESS) {
            compress = true;
            LOG.error("compress set 1");
          } else {
            if (projectionInfos == null) {
              projectionInfos = new ArrayList<ArrayList<String>>(10);
            }
            getProjectionInfo(tmpChild, projectionInfos);
          }

          if (!compress || projectionInfos == null) {
            throw new SemanticException("compress or projection error");
          }
        }

        inputFormat = "StorageEngineClient.ColumnStorageInputFormat";
        outputFormat = "StorageEngineClient.ColumnStorageHiveOutputFormat";
        tabletypesetted = true;

        serde = "StorageEngineClient.FormatStorageSerDe";
        serdesetted = true;

        break;

      case HiveParser.TOK_INDEX:

        for (int i = 1; i < numCh; i++) {
          ASTNode query = (ASTNode) ast.getChild(i);
          if (query.getToken().getType() == HiveParser.TOK_QUERY) {
            throw new SemanticException(
                ErrorMsg.CTAS_INDEX_COEXISTENCE.getMsg());
          }
        }

        if (isExt) {
          throw new SemanticException("index disallowed for extended table");
        }

        indexInfo = new indexInfoDesc();

        int indexChildNum = child.getChildCount();
        for (int i = 0; i < indexChildNum; i++) {
          ASTNode tmpChild = (ASTNode) child.getChild(i);
          if (tmpChild.getToken().getType() == HiveParser.TOK_INDEXNAME) {
            indexInfo.name = tmpChild.getChild(0).getText();

          }

          if (tmpChild.getToken().getType() == HiveParser.TOK_INDEXFIELD) {
            int indexFieldNum = tmpChild.getChildCount();

            for (int k = 0; k < indexFieldNum; k++) {
              ASTNode fieldNode = (ASTNode) tmpChild.getChild(k);

              indexInfo.fieldList.add(fieldNode.getText());
            }
          }
        }

        if (indexInfo.fieldList.size() > 1) {
          indexInfo.indexType = 2;
        } else if (indexInfo.fieldList.size() == 1) {
          String fieldName = indexInfo.fieldList.get(0);

          Iterator<FieldSchema> tblColIter = cols.iterator();
          while (tblColIter.hasNext()) {
            FieldSchema schema = tblColIter.next();
            String colName = schema.getName();
            String colType = schema.getType();
            if (colName.equalsIgnoreCase(fieldName)
                && colType.equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
              indexInfo.indexType = 0;
              break;
            }
          }
        }
        break;

      case HiveParser.TOK_PGDATA:
        pgTableFlag = true;
        tblType = 3;
        if (!isExt) {
          throw new SemanticException(
              ErrorMsg.STORE_AS_PGDATA_WITHOUT_EXTERNAL.getMsg());
        }

        break;

      case HiveParser.TOK_PROPERTIESWITH:
        if (mapPropOther == null)
          mapPropOther = new HashMap<String, String>();
        for (int propChild = 0; propChild < child.getChildCount(); propChild++) {
          ASTNode prop = (ASTNode) child.getChild(propChild);
          switch (prop.getToken().getType()) {
          case HiveParser.TOK_CHARSET:
            String charset = unescapeSQLString(prop.getChild(0).getText());

            if (Charset.isSupported(charset)) {
              mapPropOther.put(Constants.SERIALIZATION_CHARSET, charset);
            } else
              throw new SemanticException("cannot support this charset:"
                  + charset);
            break;
          case HiveParser.TOK_TABLEPROPERTY:

            String key = prop.getChild(0).getText();
            String value = prop.getChild(1).getText();
            if (key.endsWith("'") || key.endsWith("\""))
              key = unescapeSQLString(key);
            else
              key = unescapeIdentifier(key);

            if (value.endsWith("'") || value.endsWith("\""))
              value = unescapeSQLString(value);
            else
              value = unescapeIdentifier(value);

            LOG.debug("set prop: key :" + key + ", value:" + value);
            mapPropOther.put(key.toLowerCase(), value);
            break;
          default:
            throw new SemanticException("cannot set this properties"
                + prop.toStringTree());
          }
        }
        break;
      default:
        assert false;
      }
    }

    if (pgTableFlag) {
      if (mapPropOther == null)
        mapPropOther = new HashMap<String, String>();

      SessionState ss = SessionState.get();
      if (ss != null) {
    	  String pgUrl = null;
        if(conf.getVar(HiveConf.ConfVars.HIVE_PGDATA_STORAGE_URL) != "")
        	pgUrl = conf.getVar(HiveConf.ConfVars.HIVE_PGDATA_STORAGE_URL);
        else//fall back to old param
        	pgUrl = conf.getVar(HiveConf.ConfVars.PGURL);
        
        //TODO the set value should not have '' or "" now,will be improved
        if(pgUrl.contains("jdbc:postgresql://"))
        	pgUrl = pgUrl.substring(18);
        
        int colon_index = 0;
        int slash_index = 0;
        for (int i = 0; i < pgUrl.length(); i++) {
          if (pgUrl.charAt(i) == ':') {
            colon_index = i;
          }
          if (pgUrl.charAt(i) == '/') {
            slash_index = i;
          }
        }

        if (colon_index < 1 || colon_index >= pgUrl.length() || slash_index < 1
            || slash_index >= pgUrl.length()) {
          throw new SemanticException("illegal pgUrl !!!!");
        }

        String server = pgUrl.substring(0, colon_index).trim();
        String port = pgUrl.substring(colon_index + 1, slash_index).trim();
        String db_name = pgUrl.substring(slash_index + 1, pgUrl.length())
            .trim();

        mapPropOther.put(Constants.TABLE_SERVER, server);
        mapPropOther.put(Constants.TABLE_PORT, port);
        mapPropOther.put(Constants.TBALE_DB, db_name);

        String user_name = ss.getUserName();
        String pwd = ss.getPasswd();

        if (ss.getConf().get("pgdata.user") != null
            && ss.getConf().get("pgdata.user") != "") {
          user_name = ss.getConf().get("pgdata.user");

          if (ss.getConf().get("pgdata.passwd") != null
              && ss.getConf().get("pgdata.passwd") != "") {
            pwd = ss.getConf().get("pgdata.passwd");
          }
        }

        LOG.info("pgdata user: " + user_name);
        LOG.debug("passwd: " + pwd);

        mapPropOther.put(Constants.DB_URSER, user_name);
        mapPropOther.put(Constants.DB_PWD, pwd);

        mapPropOther.put(Constants.TABLE_NAME, tableName);
        mapPropOther.put(Constants.DB_TYPE, "pg");

        if (command_type != CTAS) {
          StoreAsPgdata stoAsPg = new StoreAsPgdata(tableName, cols);
          String pgSQL = stoAsPg.tdwSqlToPgsql(tableName, cols);
          mapPropOther.put(Constants.TBALE_SQL, pgSQL);
          LOG.info("sql: " + mapPropOther.get(Constants.TBALE_SQL));
          mapPropOther.put(Constants.ISCTAS, "false");
        } else {
          mapPropOther.put(Constants.ISCTAS, "true");
        }
      } else {
        throw new SemanticException("get session state error !!!!");
      }
    }

    if (ifNotExists) {
      try {
        List<String> tables = this.db.getTablesByPattern(tableName);
        if (tables != null && tables.size() > 0) {
          return null;
        }
      } catch (HiveException e) {
        e.printStackTrace();
      }
    }

    if ("formatfile".equalsIgnoreCase(conf
        .getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))
        && !isExt
        && !serdesetted && !tabletypesetted) {
      inputFormat = FORMAT_INPUT;
      outputFormat = FORMAT_OUTPUT;
      serde = FORMAT_SERDE;
      tblType = 1;
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEDEFAULTFORMATCOMPRESS)) {
        compress = true;
      }
    }

    if ("rcfile".equalsIgnoreCase(conf
        .getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))
        && !isExt
        && !serdesetted && !tabletypesetted) {
      inputFormat = RCFILE_INPUT;
      outputFormat = RCFILE_OUTPUT;
      serde = COLUMNAR_SERDE;
      tblType = 4;
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEDEFAULTRCFILECOMPRESS)) {
        compress = true;
      }
    }
    
    if (isExt && pgTableFlag && partDesc != null) {
      throw new SemanticException(
          "pg table do not allow create partitions");
    }
    
    boolean externalPartition = conf.getBoolVar(HiveConf.ConfVars.HIVESUPPORTEXTERNALPARTITION);
    if (isExt && partDesc != null) {
    	if(externalPartition){
    		
    	}
    	else{
            throw new SemanticException(
                "external table do not allow create partitions");
    	}
    }

    if (isExt && serde != null
        && serde.endsWith("StorageEngineClient.FormatStorageSerDe")) {
      throw new SemanticException("external table do not allow format storage");
    }

    if (indexInfo != null
        && (serde == null || !serde
            .endsWith("StorageEngineClient.FormatStorageSerDe"))) {
      throw new SemanticException("index only for format/column storage");
    }

    createTableDesc crtTblDesc = null;
    switch (command_type) {

    case CREATE_TABLE:
      if (mapPropOther != null && !pgTableFlag) {
        if (mapPropOther.get(Constants.TABLE_SERVER) != null
            && !mapPropOther.get(Constants.TABLE_SERVER).isEmpty()) {
          if (!isExt)
            throw new SemanticException("db external table must be external!");
          DBAndHiveCheck checker = new DBAndHiveCheck(mapPropOther, cols);
          checker.isValidCols();
        }
      }

      if (pgTableFlag) {
      }

      crtTblDesc = new createTableDesc(tableName, isExt, cols, partDesc,
          bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
          collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
          outputFormat, location, serde, mapProp, mapPropOther, ifNotExists,
          compress, tblType, projectionInfos, indexInfo, protoFileName,
          this.pb_msg_outer_name);
      validateCreateTable(crtTblDesc);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblDesc), conf));
      break;

    case CTLT:
      createTableLikeDesc crtTblLikeDesc = new createTableLikeDesc(tableName,
          isExt, location, ifNotExists, likeTableName);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblLikeDesc), conf));
      break;

    case CTAS:

      try {
        String db_name = SessionState.get().getDbName();

        Table tab = this.db.getTable(db_name, tableName, false);

        if (tab != null && ifNotExists == false) {
          throw new SemanticException(
              ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(tableName));
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      crtTblDesc = new createTableDesc(tableName, isExt, cols, partDesc,
          bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
          collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
          outputFormat, location, serde, mapProp, mapPropOther, ifNotExists,
          compress, tblType, projectionInfos, indexInfo, protoFileName,
          this.pb_msg_outer_name);
      qb.setTableDesc(crtTblDesc);

      return selectStmt;
    default:
      assert false;
    }
    return null;

  }

  private boolean hasUnionWith(ASTNode ast) throws SemanticException {
    if (ast.getType() == HiveParser.TOK_UNION
        || ast.getType() == HiveParser.TOK_UNIQUE_UNION) {
      for (Node child : ast.getChildren()) {
        if (findWithInUnion((ASTNode) ast))
          return true;
      }
    } else if (ast.getType() == HiveParser.TOK_HINTLIST) {
      return true;
    } else {
      if (ast.getChildCount() != 0) {
        for (Node child : ast.getChildren()) {
          if (child != null && hasUnionWith((ASTNode) child))
            return true;
        }
      }

    }

    return false;
  }

  private boolean findWithInUnion(ASTNode ast) throws SemanticException {
    boolean hasIt = false;
    if (ast.getType() == HiveParser.TOK_TAB) {
      ASTNode node = (ASTNode) ast.getChild(0);
      String tabName = node.getText();
      if (this.withQueries != null && this.withQueries.contains(tabName))
        hasIt = true;
    } else if (ast.getType() == HiveParser.TOK_HINTLIST) {
      hasIt = true;
    } else {
      if (ast.getChildCount() != 0) {
        for (Node child : ast.getChildren()) {
          if (child != null)
            hasIt = hasIt || findWithInUnion((ASTNode) child);
        }
      }
    }
    return hasIt;
  }

  private boolean hasWith(ASTNode ast) throws SemanticException {
    ASTNode withChild = (ASTNode) ast.getChild(0);
    if (!(withChild.getType() == HiveParser.TOK_WITH)) {
      return false;
    } else
      return true;
  }

  public boolean getHasWith() {
    return this.hasWithForParallel;
  }

  private ASTNode collectWithQueries(ASTNode ast) throws SemanticException {
    ASTNode withChild = (ASTNode) ast.getChild(0);
    if (!(withChild.getType() == HiveParser.TOK_WITH)) {
      return ast;
    }

    this.hasWith = true;
    int childCount = withChild.getChildCount() - 1;
    this.withMap = new HashMap<String, ASTNode>(childCount);
    this.withQueries = new ArrayList<String>(childCount);
    this.beProcessed = new ArrayList<Boolean>(childCount);
    this.withOps = new HashMap<String, Operator>(childCount);
    this.withCounts = new HashMap<String, Integer>(childCount);
    this.withQBEs = new ArrayList<QBExpr>(childCount);
    this.allTbls = new ArrayList<String>();

    for (int i = 0; i < childCount; i++) {
      this.beProcessed.add(false);
      withQBEs.add(null);
      ASTNode withChildi = (ASTNode) withChild.getChild(i);
      String name = withChildi.getChild(1).getText().toLowerCase();
      if (withQueries.contains(name))
        throw new SemanticException("duplicated subquery name in with");

      withQueries.add(name);
      ASTNode child0 = (ASTNode) withChildi.getChild(0);
      withMap.put(name, child0);
      withCounts.put(name, 0);

    }
    ASTNode newNode = (ASTNode) (ASTNode) withChild.getChild(childCount);

    ASTNode realInsert = null;
    for (int i = 0; i < ast.getChildCount(); i++) {
      if (ast.getChild(i).getType() == HiveParser.TOK_INSERT) {
        realInsert = (ASTNode) ast.getChild(i);
        break;
      }
    }

    if (realInsert != null) {
      for (int i = 0; i < newNode.getChildCount(); i++) {
        if (newNode.getChild(i).getType() == HiveParser.TOK_INSERT) {
          ASTNode insertNode = (ASTNode) newNode.getChild(i);
          int count = insertNode.getChildCount();
          ArrayList<ASTNode> nodes = new ArrayList<ASTNode>(count - 1);

          for (int j = count - 1; j >= 0; j--) {
            if (j == 0)
              insertNode.deleteChild(j);
            else
              nodes.add((ASTNode) insertNode.deleteChild(j));
          }
          insertNode.addChild((ASTNode) realInsert.getChild(0));
          for (int j = nodes.size() - 1; j >= 0; j--)
            insertNode.addChild(nodes.get(j));

          break;
        }
      }
    }

    return newNode;
  }

  private boolean findFather(ASTNode node, int j) {
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode nodei = (ASTNode) node.getChild(i);
      if (nodei.getType() == HiveParser.TOK_TAB
          && nodei.getChild(0).getText()
              .equalsIgnoreCase(this.withQueries.get(j))) {
        return true;
      } else
        return findFather(nodei, j);
    }

    return false;
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    isDbExternalInsert = false;
    hasWith = false;
    LOG.debug(ast.toStringTree());
    reset();

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_DIVIDE_ZERO_RETURN_NULL)) {
      checkdividezero(ast);
    }
    transformcountstart2count1(ast);

    QB qb = new QB(null, null, false);
    this.qb = qb;
    this.ast = ast;
    ASTNode child = ast;

    if (conf.getBoolean("changeASTTreesForOptimizeInsertWithoutUnion", true))
      changeASTTreesForOptimizeInsertWithoutUnion(ast);

    viewsExpanded = new ArrayList<String>();

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Starting Semantic Analysis");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Starting Semantic Analysis");

    if (SessionState.get() != null) {
      SessionState.get().setCreateQueryFlag(false);
      LOG.debug("set createQueryFlag false");
    }

    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      SessionState.get().setCreateQueryFlag(true);
      LOG.debug("set createQueryFlag true");

      if ((child = analyzeCreateTable(ast, qb)) == null)
        return;
    }
    if (ast.getToken().getType() == HiveParser.TOK_CREATEVIEW) {
      SessionState.get().setCreateQueryFlag(true);
      LOG.debug("set createQueryFlag true");

      child = analyzeCreateView(ast, qb);
      if (child == null) {
        return;
      }
      viewSelect = child;
      if (createVwDesc.getViewName().contains("::"))
        viewsExpanded.add(createVwDesc.getViewName());
      else
        viewsExpanded.add(SessionState.get().getDbName() + "::"
            + createVwDesc.getViewName());
    }

    ExistsTranslator exiTran = new ExistsTranslator();
    exiTran.changeASTTreesForExists(ast);

    if (!conf.getVar(HiveConf.ConfVars.HIVEOPTIMIZECUBEROLLUP)
        .equalsIgnoreCase("true")) {
      RollupAndCubeTranslator racTran = new RollupAndCubeTranslator();
      racTran.changeASTTreesForRC(ast);
    }

    this.hasWithForParallel = hasWith(ast);

    child = ast;

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start do phase 1");
    doPhase1(ast, qb, initPhase1Ctx());
    
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed phase 1 of Semantic Analysis");

    boolean selectWithNoFrom = isSelectWithNoFrom(qb);

    if (selectWithNoFrom) {
      LOG.debug("select with no from !!!!!!!");
      if (this.ipinfoExist) {
        throw new SemanticException(
            "udf ipinfo doesnot support in No-From query, please add from statement or"
                + " use sql: select * from ip_table where ip>=start_ip and ip<=end_ip.");
      }

      QBParseInfo qbp = qb.getParseInfo();
      TreeSet<String> ks = new TreeSet<String>();
      ks.addAll(qbp.getClauseNames());

      for (String dest : ks) {
        LOG.debug("dest name is " + dest);
        getMetaData(qb);
        Operator selOp = genSelectPlanForNoFrom(dest, qb);
        Operator fsOp = genLocalFileSinkPlan(dest, qb, selOp);

        filesinkop = fsOp;
        genFetchTaskForSelectNoFrom();

        LocalSelectWork lsWork = new LocalSelectWork(selOp);
        Task<LocalSelectWork> lsTask = TaskFactory.get(lsWork, this.conf);
        getRootTasks().add(lsTask);
      }

      return;
    }

    isAllDBExternalTable = true;

    getAllTableName(qb);

    if (allTableMap != null && allTableMap.isEmpty()) {
      isAllDBExternalTable = false;
      LOG.info("table alias is empty");
    }

    if (SessionState.get() != null)
      multiHdfsInfo.setQueryid(SessionState.get().getQueryId());
    
    //ArrayList<String> dblList = new ArrayList<String>(allTableMap.keySet());
    //boolean insertdir = false;
    
    if (allTableMap != null && !allTableMap.isEmpty()) {
      try {
        ArrayList<String> dblList = new ArrayList<String>(allTableMap.keySet());
        String db_name = null;
        if (qb.isCTAS()) {
          db_name = SessionState.get().getDbName().toLowerCase();
          String tb_name = qb.getTableDesc().getTableName();
          dblList.add(db_name + "/" + tb_name);
        }

        QBParseInfo qbp = qb.getParseInfo();

        boolean insertdir = false;
        for (String name : qbp.getClauseNamesForDest()) {
          ASTNode asttmp = qbp.getDestForClause(name);
          if (asttmp.getToken().getType() == HiveParser.TOK_TABDEST) {
            db_name = getDbnameFromAST((ASTNode) asttmp.getChild(0));
            dblList.add(db_name + "/tmp");
          }

          if (asttmp.getToken().getType() == HiveParser.TOK_DIR) {
            if (((ASTNode) asttmp.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE) {
              continue;
            }

            insertdir = true;
            break;
          }
        }
        if (!insertdir) {
          multiHdfsInfo.checkMultiHdfsEnable(dblList);
        }
      } catch (MetaException e) {
        throw new SemanticException("check multi hdfs enable error:"
            + e.getMessage());
      }
    }

    HashMap<String, Table> extTbl = new HashMap<String, Table>();
    HashMap<String, String> tableNameMap = new HashMap<String, String>();
    if (allTableMap != null) {
      for (Entry<String, Table> entry : allTableMap.entrySet()) {
        String tab_name = entry.getKey();
        Table tab = entry.getValue();
  
        String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
        String dbType = tab.getSerdeParam(Constants.DB_TYPE);
        String tableName = tab.getSerdeParam(Constants.TABLE_NAME);
        String tableSql = tab.getSerdeParam(Constants.TBALE_SQL);
        LOG.debug("++++++++++++++" + tableServer);
        LOG.debug("tableName: " + tableName);
  
        if (tableServer != null
            && !tableServer.isEmpty()
            && (dbType.equalsIgnoreCase("pgsql") || dbType.equalsIgnoreCase("pg"))) {
          if (tableName == null || tableName.isEmpty()) {
            if (tableSql != null && tableSql.isEmpty() || tableSql == null) {
              LOG.info("Both table name and table sql missed");
              isAllDBExternalTable = false;
              break;
            } else {
              LOG.info("table name missed, cannot push down to pg");
              isAllDBExternalTable = false;
              break;
            }
          }
          tableNameMap.put(tab_name, tableName);
          extTbl.put(tab_name, tab);
          continue;
        } else {
          isAllDBExternalTable = false;
          break;
        }
      }
    }

    if (ctx.getExplain()) {
      isAllDBExternalTable = false;
    }

    if (isAllDBExternalTable && allTableMap != null && !allTableMap.isEmpty()) {
      SqlCondPushDown sqlCond = new SqlCondPushDown(extTbl, tableNameMap);
      String tdwSql = ctx.getTokenRewriteStream().toString(
          this.ast.getTokenStartIndex(), this.ast.getTokenStopIndex());
      String pgSql = tdwSql;
      LOG.info("tdwsql: " + tdwSql);
      int ret = -1;

      ExtractConfig config = new ExtractConfig();
      config.setConf(conf);

      String tmpDir = conf.getVar(HiveConf.ConfVars.SCRATCHDIR);
      String filePath = tmpDir + "/" + conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "_" + Utilities.randGen.nextInt();
      config.setFilePath(filePath);

      Properties p = conf.getAllProperties();

      
      String hdfsDefalutName = p.getProperty("fs.defaultFS");
      if(hdfsDefalutName == null){
        hdfsDefalutName = p.getProperty("fs.default.name");
      }
      

      pgTmpDir = hdfsDefalutName + filePath;

      pgDataTmpFilePathes.put("pg_database/pg_table", hdfsDefalutName
          + filePath);
      LOG.info("pgTmpDir: " + pgTmpDir);

      if (extTbl.keySet().iterator().hasNext()) {
        Table tbl = extTbl.get(extTbl.keySet().iterator().next());
        config.setHost(tbl.getSerdeParam(Constants.TABLE_SERVER));
        config.setPort(tbl.getSerdeParam(Constants.TABLE_PORT));
        config.setDbType(tbl.getSerdeParam(Constants.DB_TYPE));
        config.setDbName(tbl.getSerdeParam(Constants.TBALE_DB));
        config.setUser(tbl.getSerdeParam(Constants.DB_URSER));
        config.setPwd(tbl.getSerdeParam(Constants.DB_PWD));
      }

      pgSql = sqlCond.tdwSqlToPgsql(tdwSql);
      config.setSql(pgSql);
      LOG.info("pgsql: " + pgSql);

      try {
        IFetchSize fetch = new MemoryAdaptiveFetchSize(config);
        config.setBufferLimit(fetch.computeFetchSize(config));
      } catch (HiveException e) {
        LOG.error("connection to PG error: " + e.getMessage());
        throw new SemanticException("Cannot connect to BI database: "
            + e.getMessage(), e);
      }
      sqlCond.setConfig(config);

      ret = sqlCond.sendSqlToPg(pgSql);
      if (ret != 0) {
        isAllDBExternalTable = false;
      }
    }

    if (canUseIndex) {
      if (ast.toStringTree().toLowerCase().contains("tok_function")) {
        canUseIndex = false;
      }
    }

    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed phase 1 of Semantic Analysis");

    if (this.hasWith) {
      for (int i = 0; i < this.beProcessed.size(); i++) {
        this.beProcessed.set(i, false);
      }
    }

    if (allTableMap != null && !allTableMap.isEmpty()) {
      Set<String> tblNameSet = allTableMap.keySet();
      for (String tblName : tblNameSet) {
        firstItemMap.put(tblName, true);
      }
    }

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start getting MetaData in Semantic Analysis");
    getMetaData(qb);
	
    if(!multiHdfsInfo.isMultiHdfsEnable()){
      boolean isAllTableOnDefaultHdfs = checkAllTableOnDefaultHdfs(allTableMap);
      if(isAllTableOnDefaultHdfs){
        //LOG.info("all table on the default hdfs");
        //try
        //{
        //  if (!insertdir) {
        //    multiHdfsInfo.checkMultiHdfsEnable(dblList);
        //  }
        //} catch (MetaException e) {
        //  throw new SemanticException("check multi hdfs enable error:"
        //      + e.getMessage());
        //}
      }
      else{
        LOG.info("not all table on the default hdfs");
        multiHdfsInfo.setMultiHdfsEnable(!isAllTableOnDefaultHdfs);
      }   
    }
	
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed getting MetaData in Semantic Analysis");
    if (SessionState.get() != null)
      SessionState.get().ssLog(
          "Completed getting MetaData in Semantic Analysis");
    if (isDbExternalInsert)
      return;

    if (this.hasWith) {
      for (int i = 0; i < this.beProcessed.size(); i++) {
        this.beProcessed.set(i, false);
      }
    }

    Operator sinkOp = genPlan(qb);

    boolean isCP = isCartesianProduct(qb);
    if (isCP) {
      conf.setBoolean("hive.sql.cartesian.product", true);
    } else {
      conf.setBoolean("hive.sql.cartesian.product", false);
    }

    if (createVwDesc != null) {
      saveViewDefinition(sinkOp);
      ctx.setResDir(null);
      ctx.setResFile(null);
      return;
    }

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed gen Op Plan");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed gen Op Plan");

    ParseContext pCtx = new ParseContext(conf, qb, child, aliasToPruner,
        opToPartPruner, aliasToSamplePruner, topOps, topSelOps, opParseCtx,
        joinContext, topToTable, loadTableWork, loadFileWork, ctx,
        idToTableNameMap, destTableId, uCtx, listMapJoinOpsNoReducer);

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start optimize");
    Optimizer optm = new Optimizer();
    optm.setPctx(pCtx);
    optm.initialize(conf);
    pCtx = optm.optimize();
    init(pCtx);
    qb = pCtx.getQB();

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed optimize");

    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed optimizer");

    if (this.hasWith) {
      for (int i = 0; i < this.beProcessed.size(); i++) {
        this.beProcessed.set(i, false);
      }
    }
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start partition pruning");
    genPartitionPruners(qb);

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed partition pruning");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed partition pruning");
    
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start sample pruning");
    genSamplePruners(qb);
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed sample pruning");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed sample pruning");

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Filter op Start!");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Filter op Start!");
    this.printTreeOftopOps();
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Filter op Over!");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Filter op Over!");

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "start mr plan generation");
    
    genMapRedTasks(qb);

    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Completed mr plan generation");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Completed plan generation");

    return;
  }

  
  private boolean checkAllTableOnDefaultHdfs(Map<String, Table> tableMap){
    String defaultSchema = multiHdfsInfo.getDefaultNNSchema();
    LOG.info("defaultSchema=" + defaultSchema);
    if(defaultSchema == null){
      return false;
    }
    
    for(Entry<String, Table> e:tableMap.entrySet()){
      if(e.getValue() != null){
        Table t = e.getValue();
        URI location = t.getDataLocation();
        if(location == null){
          continue;
        }
        else{
          String tblPath = location.toString();
          if(tblPath != null && !tblPath.startsWith(defaultSchema)){
            return false;
          }
        }
      }
    }
    return true;
  }
  
  private void checkdividezero(ASTNode ast) {
    if ("/".equalsIgnoreCase(ast.getText())) {
      ast.token.setText("//");
    }
    for (int i = 0; i < ast.getChildCount(); i++) {
      checkdividezero((ASTNode) ast.getChild(i));
    }
  }

  private void transformcountstart2count1(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() > 0) {
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode ast1 = (ASTNode) ast.getChild(i);
        if (ast1.getType() == HiveParser.TOK_FUNCTIONSTAR
            && "count".equalsIgnoreCase(ast1.getChild(0).getText())) {
          ASTNode ast2 = ASTNode.get(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
          ast2.addChild(ASTNode.get(HiveParser.Identifier, "count"));
          ast2.addChild(ASTNode.get(HiveParser.Number, "1"));
          ast.setChild(i, ast2);
        } else if (ast1.getType() == HiveParser.TOK_FUNCTION
            && "ipinfo".equalsIgnoreCase(ast1.getChild(0).getText())) {
          if (ast1.getChildCount() == 4) {
            changeASTTreeForIpInfoFuncTableNameToPath(ast1);
          } else {
            throw new SemanticException(
                "udf:ipinfo should be ipinfo (tableName, ip, index) ");
          }
        }
      }
      for (int i = 0; i < ast.getChildCount(); i++) {
        transformcountstart2count1((ASTNode) ast.getChild(i));
      }
    }
  }

  public static ColumnInfo processTableAlias(ASTNode expr, RowResolver input,
      QB qb) throws SemanticException {
    QBParseInfo qbp = qb.getParseInfo();
    ColumnInfo colInfo = null;

    if (input.getTableNames().size() > 1)
      return colInfo;

    String colname = "";
    if (expr.getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = expr.getChild(0).getText();
      LOG.debug("expr colname:" + colname);
    } else if (expr.getType() == HiveParser.DOT
        && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
      String UserTableAlias = expr.getChild(0).getChild(0).getText();

      String dbname = null;
      if (expr.getChild(0).getChildCount() == 2)
        dbname = expr.getChild(0).getChild(1).getText();

      if (dbname == null)
        dbname = SessionState.get().getDbName();

      Iterator<String> tableNames = input.getTableNames().iterator();
      String tableName = "";
      if (tableNames.hasNext())
        tableName = tableNames.next();
      else
        return colInfo;

      boolean testTable = false;
      String fullAlias = "";

      if (tableName == null || tableName.equalsIgnoreCase("")) {
        if (qb.existsUserAlias(UserTableAlias)) {
          if (qb.getSubqAliases().contains(UserTableAlias)) {
            testTable = true;
          } else {
            fullAlias = qb.getTableRefFromUserAlias(UserTableAlias).getDbName()
                + "/"
                + qb.getTableRefFromUserAlias(UserTableAlias).getTblName()
                + "#" + UserTableAlias;

            if (qb.getTabAliases().contains(fullAlias.toLowerCase())) {
              testTable = true;
            }

          }
        } else if (qb.exisitsDBTB(dbname + "/" + UserTableAlias)) {
          testTable = true;
        } else
          ;
      } else
        ;

      if (!testTable)
        return colInfo;

      colname = expr.getChild(1).getText();

      LOG.debug("expr colname:" + colname);
    } else {
      return colInfo;
    }

    Iterator<String> tableNames = input.getTableNames().iterator();
    String tableName = "";
    if (tableNames.hasNext())
      tableName = tableNames.next();
    else
      return colInfo;

    LOG.debug("tableName:" + tableName);
    HashMap<String, ColumnInfo> fieldMap = input.getFieldMap(tableName);

    String find = "(?<=^\\(\\. \\(TOK_TABLE_OR_COL .{1,255}?\\) ).*(?=\\)$)";
    Pattern p = Pattern.compile(find, Pattern.CASE_INSENSITIVE);

    find = "(?<=^\\(TOK_TABLE_OR_COL ).*(?=\\)$)";
    Pattern p1 = Pattern.compile(find, Pattern.CASE_INSENSITIVE);

    Matcher match = null;

    for (Map.Entry<String, ColumnInfo> entry : fieldMap.entrySet()) {
      String field = entry.getKey();
      ColumnInfo valueInfo = entry.getValue();

      LOG.debug("field:" + field);

      match = p.matcher(field);
      if (null == match)
        return colInfo;

      if (match.find())
        field = match.group(0);

      match = p1.matcher(field);
      if (null == match)
        return colInfo;

      if (match.find())
        field = match.group(0);

      if (field.equalsIgnoreCase(colname)) {
        return valueInfo;
      }
    }

    return colInfo;
  }

  public exprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input, QB qb)
      throws SemanticException {
    return genExprNodeDesc(expr, input, qb, -1, conf);
  }

  private void saveViewDefinition(Operator sinkOp) throws SemanticException {

    List<FieldSchema> derivedSchema = convertRowSchemaToViewSchema(opParseCtx
        .get(sinkOp).getRR());
    validateColumnNameUniqueness(derivedSchema);

    List<FieldSchema> imposedSchema = createVwDesc.getSchema();
    if (imposedSchema != null) {
      int explicitColCount = imposedSchema.size();
      int derivedColCount = derivedSchema.size();
      if (explicitColCount != derivedColCount) {
        throw new SemanticException(
            ErrorMsg.VIEW_COL_MISMATCH.getMsg(viewSelect));
      }
    }

    String originalText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());
    createVwDesc.setViewOriginalText(originalText);

    unparseTranslator.applyTranslation(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());

    if (imposedSchema != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ");
      int n = derivedSchema.size();
      for (int i = 0; i < n; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        FieldSchema fieldSchema = derivedSchema.get(i);
        fieldSchema = new FieldSchema(fieldSchema);
        derivedSchema.set(i, fieldSchema);
        sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName()));
        sb.append(" AS ");
        String imposedName = imposedSchema.get(i).getName();
        sb.append(HiveUtils.unparseIdentifier(imposedName));
        fieldSchema.setName(imposedName);
        fieldSchema.setComment(imposedSchema.get(i).getComment());
      }
      sb.append(" FROM (");
      sb.append(expandedText);
      sb.append(") ");
      sb.append(HiveUtils.unparseIdentifier(createVwDesc.getViewName()));
      expandedText = sb.toString();
    }

    createVwDesc.setSchema(derivedSchema);
    createVwDesc.setViewExpandedText(expandedText);
  }

  private List<FieldSchema> convertRowSchemaToViewSchema(RowResolver rr) {
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      String colName = rr.reverseLookup(colInfo.getInternalName())[1];
      fieldSchemas.add(new FieldSchema(colName,
          colInfo.getType().getTypeName(), null));
    }
    return fieldSchemas;
  }

  @SuppressWarnings("nls")
  public static exprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input,
      QB qb, int tag, Configuration conf) throws SemanticException {

    ColumnInfo colInfo;

    if (tag < 0)
      colInfo = input.get("", expr.toStringTree());
    else
      colInfo = input.get("", tag + "." + expr.toStringTree());
    if (colInfo == null) {
      LOG.debug("colInfo is null for " + expr.toStringTree());
      colInfo = input.get(null, expr.toStringTree());
      if (colInfo == null)
        LOG.debug("colInfo is null 2 for " + expr.toStringTree());
    }
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(expr);
      if (source != null)
        unparseTranslator.addCopyTranslation(expr, source);
      return new exprNodeColumnDesc(colInfo.getType(),
          colInfo.getInternalName(), colInfo.getTabAlias(),
          colInfo.getIsPartitionCol());
    }

    colInfo = processTableAlias(expr, input, qb);

    if (colInfo != null) {
      LOG.debug("colInfo exist2");
      String[] tmp = input.reverseLookup(colInfo.getInternalName());
      StringBuilder replacementText = new StringBuilder();
      String tnew = null;
      if (tmp[0] != null) {
        String[] tnameTmp = HiveUtils.unparseIdentifier(tmp[0]).split("#");

        if (tnameTmp.length == 2)
          tnew = tnameTmp[1];
        else
          tnew = tnameTmp[0].replace("/", "::");
        replacementText.append(tnew);
        replacementText.append(".");
        replacementText.append(HiveUtils.unparseIdentifier(tmp[1]));
        LOG.debug("Added to unparseTranslator: " + expr.toStringTree()
            + "  TO  " + replacementText);
        unparseTranslator.addTranslation(expr, replacementText.toString());
      } else {
        unparseTranslator.addTranslation(expr,
            HiveUtils.unparseIdentifier(tmp[1]));
      }

      return new exprNodeColumnDesc(colInfo.getType(),
          colInfo.getInternalName(), colInfo.getTabAlias(),
          colInfo.getIsPartitionCol());
    }

    TypeCheckCtx tcCtx = new TypeCheckCtx(input, qb);
    tcCtx.setUnparseTranslator(unparseTranslator);

    HashMap<Node, Object> nodeOutputs = TypeCheckProcFactory.genExprNode(expr,
        tcCtx, conf);

    exprNodeDesc desc = (exprNodeDesc) nodeOutputs.get(expr);
    if (desc == null) {
      throw new SemanticException(tcCtx.getError());
    }

    if (!unparseTranslator.isEnabled()) {
      return desc;
    }

    for (Map.Entry<Node, Object> entry : nodeOutputs.entrySet()) {
      if (!(entry.getKey() instanceof ASTNode)) {
        continue;
      }
      if (!(entry.getValue() instanceof exprNodeColumnDesc)) {
        continue;
      }
      ASTNode node = (ASTNode) entry.getKey();
      exprNodeColumnDesc columnDesc = (exprNodeColumnDesc) entry.getValue();
      if ((columnDesc.getTabAlias() == null)
          || (columnDesc.getTabAlias().length() == 0)) {
        continue;
      }

      String[] tmp = input.reverseLookup(columnDesc.getColumn());
      StringBuilder replacementText = new StringBuilder();

      String tnew = null;
      if (tmp[0] != null) {
        String[] tnameTmp = HiveUtils.unparseIdentifier(tmp[0]).split("#");

        if (tnameTmp.length == 2)
          tnew = tnameTmp[1];
        else
          tnew = tnameTmp[0].replace("/", "::");
        replacementText.append(tnew);
        replacementText.append(".");
        replacementText.append(HiveUtils.unparseIdentifier(tmp[1]));
        LOG.debug("genExprNodeDesc add1: " + node.toStringTree());
        LOG.debug("replacementText: " + replacementText);
        unparseTranslator.addTranslation(node, replacementText.toString());
      } else {
        LOG.debug("genExprNodeDesc add2: " + node.toStringTree());
        LOG.debug("replacementText: " + HiveUtils.unparseIdentifier(tmp[1]));
        unparseTranslator.addTranslation(node,
            HiveUtils.unparseIdentifier(tmp[1]));
      }
    }

    return desc;
  }

  static String getTabAliasForCol(QBMetaData qbm, String colName, ASTNode pt)
      throws SemanticException {
    String tabAlias = null;
    boolean found = false;

    for (Map.Entry<String, TablePartition> ent : qbm.getAliasToTable()
        .entrySet()) {
      for (FieldSchema field : ent.getValue().getAllCols()) {
        if (colName.equalsIgnoreCase(field.getName())) {
          if (found) {
            throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(pt));
          }

          found = true;
          tabAlias = ent.getKey();
        }
      }
    }
    return tabAlias;
  }

  public void validate() throws SemanticException {

    for (Task<? extends Serializable> rootTask : rootTasks)
      validate(rootTask);
  }

  private void validate(Task<? extends Serializable> task)
      throws SemanticException {
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      mapredWork work = (mapredWork) task.getWork();

    }

    if (task.getChildTasks() == null)
      return;

    for (Task<? extends Serializable> childTask : task.getChildTasks())
      validate(childTask);
  }

  @Override
  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public String getFullAlias(QB qb, ASTNode expr) throws SemanticException {
    assert (expr.getType() == HiveParser.TOK_TABLE_OR_COL || expr.getType() == HiveParser.TOK_ALLCOLREF);

    if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
      if (expr.getChildCount() == 0)
        return null;
    }

    LOG.debug("getfullAlias : expr : " + expr.toStringTree());
    String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(
        expr.getChild(0).getText()).toLowerCase();
    String db = null;
    boolean setDb = false;
    String fullAlias = null;
    if (expr.getChildCount() == 2) {
      db = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText())
          .toLowerCase();
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
        fullAlias = fullAlias + "#" + qb.getUserAliasFromDBTB(fullAlias).get(0);
      }

      LOG.debug("setDb....fullAlias is : " + fullAlias);
    } else {
      if (qb.existsUserAlias(tableOrCol)) {
        LOG.debug("do not setDb....fullAlias is : " + fullAlias);
        if (qb.getSubqAliases().contains(tableOrCol.toLowerCase())) {
          fullAlias = tableOrCol;
          LOG.debug("a sub alias....fullAlias is : " + fullAlias);
        } else {
          fullAlias = qb.getTableRefFromUserAlias(tableOrCol).getDbName() + "/"
              + qb.getTableRefFromUserAlias(tableOrCol).getTblName() + "#"
              + tableOrCol;
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
              LOG.debug("table : " + fullAlias + "  has more than one alias : "
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

    return fullAlias;

  }

  public Map<String, String> getAllDBDataTmpFiles() {
    return dbDataTmpFilePathes;
  }

  public Map<String, String> getAllPgDataTmpFiles() {
    return pgDataTmpFilePathes;
  }

  public void getAllSubqTableName(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getAllTableName(qbexpr.getQB());
    } else {
      getAllSubqTableName(qbexpr.getQBExpr1());
      getAllSubqTableName(qbexpr.getQBExpr2());
    }
  }

  public void getAllTableName(QB qb) throws SemanticException {
    for (String alias : qb.getTabAliases()) {
      String tab_name = qb.getTabNameForAlias(alias);
      String db_name = qb.getTableRef(alias).getDbName();

      LOG.debug("table name: " + tab_name);
      LOG.debug("db name: " + db_name);

      Table tab = null;

      int optimizeLevel = conf.getInt("hive.semantic.analyzer.optimizer.level",
          2);
      switch (optimizeLevel) {
      case 0:
        try {
          this.db.getDatabase(db_name);
        } catch (Exception e) {
          throw new SemanticException("get database : " + db_name
              + " error,make sure it exists!");
        }

        try {
          tab = this.db.getTable(db_name, tab_name);
        } catch (InvalidTableException ite) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(db_name
              + "::" + tab_name));
        } catch (HiveException e) {
          throw new SemanticException(e.getMessage());
        }
        if (allTableMap != null) {
          allTableMap.put(db_name + "/" + tab_name, tab);
        }
        break;

      case 1:
      case 2:

      default:
        if (allTableMap != null && !allTableMap.isEmpty()) {
          tab = allTableMap.get(db_name + "/" + tab_name);
        }

        if (tab == null) {
          try {
            this.db.getDatabase(db_name);
          } catch (Exception e) {
            throw new SemanticException("get database : " + db_name
                + " error,make sure it exists!");
          }

          try {
            tab = this.db.getTable(db_name, tab_name);
          } catch (InvalidTableException ite) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(db_name
                + "::" + tab_name));
          } catch (HiveException e) {
            throw new SemanticException(e.getMessage());
          }

          if (allTableMap != null) {
            allTableMap.put(db_name + "/" + tab_name, tab);
          }
        }
        break;
      }
    }

    for (String alias : qb.getSubqAliases()) {
      LOG.debug("subq alias: " + alias);
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      getAllSubqTableName(qbexpr);
    }

    return;
  }
}
