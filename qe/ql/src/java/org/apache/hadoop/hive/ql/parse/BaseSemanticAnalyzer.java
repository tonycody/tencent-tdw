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
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseDriver.HiveParserX;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;

public abstract class BaseSemanticAnalyzer {
  protected final Hive db;
  protected final HiveConf conf;
  protected List<Task<? extends Serializable>> rootTasks;
  protected Task<? extends Serializable> fetchTask;
  protected boolean fetchTaskInit;
  protected Log LOG;
  protected final LogHelper console;

  protected Context ctx;
  protected HashMap<String, String> idToTableNameMap;

  protected int numOfHashPar;

  public BaseSemanticAnalyzer(HiveConf conf) throws SemanticException {
    try {
      this.conf = conf;
      db = Hive.get(conf);
      rootTasks = new ArrayList<Task<? extends Serializable>>();
      LOG = LogFactory.getLog(this.getClass().getName());
      console = new LogHelper(LOG);
      this.idToTableNameMap = new HashMap<String, String>();
      numOfHashPar = conf.getInt("hive.hashPartition.num", 500);
      if (numOfHashPar <= 0)
        throw new MetaException(
            "Hash Partition Number should be Positive Integer!");
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public HashMap<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public abstract void analyzeInternal(ASTNode ast) throws SemanticException;

  public void analyze(ASTNode ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    changeASTTreesForNN(ast);
    analyzeInternal(ast);
  }

  private void changeASTTreesForNN(ASTNode ast) {
    int child_count = ast.getChildCount();
    List<Integer> lists = new ArrayList<Integer>();
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {
      ASTNode node1 = (ASTNode) ast.getChild(child_pos);
      if (node1.getToken().getType() == HiveParser.TOK_NNUMBER) {
        lists.add(child_pos);
      }
    }
    for (int i : lists) {
      ASTNode node1 = (ASTNode) ast.getChild(i);
      ASTNode tmpast = ASTNode.get(HiveParser.Number, node1.getChild(0)
          .getText() + node1.getChild(1).getText());
      ast.setChild(i, tmpast);
    }

    for (int child_pos = 0; child_pos < child_count; ++child_pos) {

      changeASTTreesForNN((ASTNode) ast.getChild(child_pos));
    }

  }

  public static Table getTableMeta(Hive db, String dbName, String tableName,
      Map<String, Table> tbls, Map<String, Boolean> tblFirstMap,
      int optimizeLevel) throws HiveException {
    Table tbl = null;
    if (tbls != null) {
      Boolean isFirst = null;
      Table tblOld = null;
      if (tbls != null && !tbls.isEmpty()) {
        tblOld = tbls.get(dbName + "/" + tableName);
      }

      if (tblOld != null) {
        if (tblFirstMap != null) {
          isFirst = tblFirstMap.get(dbName + "/" + tableName);
          if (isFirst != null && isFirst.booleanValue()) {
            tbl = tblOld;
            tblFirstMap.put(dbName + "/" + tableName, false);
          } else {
            tbl = db.cloneTable(tblOld.getTTable());
          }
        }
      }
    }

    if (tbl == null) {
      try {
        db.getDatabase(dbName);
      } catch (Exception e) {
        throw new SemanticException("get database error : " + dbName
            + " ,make sure it exists!");
      }

      tbl = db.getTable(dbName, tableName);
      if (tbls != null) {
        tbls.put(dbName + "/" + tableName, tbl);
        if (tblFirstMap != null) {
          tblFirstMap.put(dbName + "/" + tableName, false);
        }
      }

    }

    return tbl;
  }

  public void validate() throws SemanticException {
  }

  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  public Task<? extends Serializable> getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(Task<? extends Serializable> fetchTask) {
    this.fetchTask = fetchTask;
  }

  public boolean getFetchTaskInit() {
    return fetchTaskInit;
  }

  public void setFetchTaskInit(boolean fetchTaskInit) {
    this.fetchTaskInit = fetchTaskInit;
  }

  protected void reset() {
    rootTasks = new ArrayList<Task<? extends Serializable>>();
  }

  public static String stripQuotes(String val) throws SemanticException {
    if ((val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'')
        || (val.charAt(0) == '\"' && val.charAt(val.length() - 1) == '\"')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static String charSetString(String charSetName, String charSetString)
      throws SemanticException {
    try {
      charSetName = charSetName.substring(1);
      if (charSetString.charAt(0) == '\'')
        return new String(unescapeSQLString(charSetString).getBytes(),
            charSetName);
      else {
        assert charSetString.charAt(0) == '0';
        assert charSetString.charAt(1) == 'x';
        charSetString = charSetString.substring(2);

        byte[] bArray = new byte[charSetString.length() / 2];
        int j = 0;
        for (int i = 0; i < charSetString.length(); i += 2) {
          int val = Character.digit(charSetString.charAt(i), 16) * 16
              + Character.digit(charSetString.charAt(i + 1), 16);
          if (val > 127)
            val = val - 256;
          bArray[j++] = new Integer(val).byteValue();
        }

        String res = new String(bArray, charSetName);
        return res;
      }
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
  }

  public static String unescapeIdentifier(String val) {
    if (val == null) {
      return null;
    }
    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {

    Character enclosure = null;

    StringBuilder sb = new StringBuilder(b.length());
    for (int i = 0; i < b.length(); i++) {

      char currentChar = b.charAt(i);
      if (enclosure == null) {
        if (currentChar == '\'' || b.charAt(i) == '\"') {
          enclosure = currentChar;
        }
        continue;
      }

      if (enclosure.equals(currentChar)) {
        enclosure = null;
        continue;
      }

      if (currentChar == '\\' && (i + 4 < b.length())) {
        char i1 = b.charAt(i + 1);
        char i2 = b.charAt(i + 2);
        char i3 = b.charAt(i + 3);
        if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
            && (i3 >= '0' && i3 <= '7')) {
          byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
          byte[] bValArr = new byte[1];
          bValArr[0] = bVal;
          String tmp = new String(bValArr);
          sb.append(tmp);
          i += 3;
          continue;
        }
      }

      if (currentChar == '\\' && (i + 2 < b.length())) {
        char n = b.charAt(i + 1);
        switch (n) {
        case '0':
          sb.append("\0");
          break;
        case '\'':
          sb.append("'");
          break;
        case '"':
          sb.append("\"");
          break;
        case 'b':
          sb.append("\b");
          break;
        case 'n':
          sb.append("\n");
          break;
        case 'r':
          sb.append("\r");
          break;
        case 't':
          sb.append("\t");
          break;
        case 'Z':
          sb.append("\u001A");
          break;
        case '\\':
          sb.append("\\");
          break;
        case '%':
          sb.append("\\%");
          break;
        case '_':
          sb.append("\\_");
          break;
        default:
          sb.append(n);
        }
        i++;
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  public Set<ReadEntity> getInputs() {
    return new LinkedHashSet<ReadEntity>();
  }

  public Set<WriteEntity> getOutputs() {
    return new LinkedHashSet<WriteEntity>();
  }

  protected List<FieldSchema> getColumns(ASTNode ast) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode) ast.getChild(i);

      col.setName(unescapeIdentifier(child.getChild(0).getText()));
      ASTNode typeChild = (ASTNode) (child.getChild(1));
      col.setType(getTypeStringFromAST(typeChild));

      if (child.getChildCount() == 3)
        col.setComment(unescapeSQLString(child.getChild(2).getText()));
      colList.add(col);
    }
    return colList;
  }

  protected List<String> getColumnNames(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(unescapeIdentifier(child.getText()));
    }
    return colList;
  }

  protected List<Order> getColumnNamesOrder(ASTNode ast) {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC)
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()),
            1));
      else
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()),
            0));
    }
    return colList;
  }

  protected static String getTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    if (typeNode == null)
      return null;
    switch (typeNode.getType()) {
    case HiveParser.TOK_LIST:
      return Constants.LIST_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
    case HiveParser.TOK_MAP:
      return Constants.MAP_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
    case HiveParser.TOK_STRUCT:
      return getStructTypeStringFromAST(typeNode);
    default:
      return DDLSemanticAnalyzer.getTypeName(typeNode.getType());
    }
  }

  private static String getStructTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = Constants.STRUCT_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0)
      throw new SemanticException("empty struct not allowed.");
    for (int i = 0; i < children; i++) {
      ASTNode child = (ASTNode) typeNode.getChild(i);
      typeStr += unescapeIdentifier(child.getChild(0).getText()) + ":";
      typeStr += getTypeStringFromAST((ASTNode) child.getChild(1));
      if (i < children - 1)
        typeStr += ",";
    }

    typeStr += ">";
    return typeStr;
  }

  private static String getUnionTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = Constants.UNION_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty union not allowed.");
    }
    for (int i = 0; i < children; i++) {
      typeStr += getTypeStringFromAST((ASTNode) typeNode.getChild(i));
      if (i < children - 1) {
        typeStr += ",";
      }
    }
    typeStr += ">";
    return typeStr;
  }

  public static class InsertPartDesc {
    private List<String> partList;
    private List<String> subPartList;
    private PartRefType partType;
    private int errorLimit = 500;

    public InsertPartDesc() {
      partList = new ArrayList<String>();
      subPartList = new ArrayList<String>();
      partType = null;
      errorLimit = 500;
    }

    public InsertPartDesc(List<String> partList, List<String> subPartList,
        PartRefType partType, int errorLimit) {
      this.partList = partList;
      this.subPartList = subPartList;
      this.partType = partType;
      this.errorLimit = errorLimit;
    }

    public int getErrorLimit() {
      return errorLimit;
    }

    public List<String> getPartList() {
      return partList;
    }

    public List<String> getSubPartList() {
      return subPartList;
    }

    public PartRefType getPartType() {
      return partType;
    }

    public void setPartList(List<String> partList) {
      this.partList = partList;
    }

    public void setSubPartList(List<String> subPartList) {
      this.subPartList = subPartList;
    }

    public void addPart(String part) {
      partList.add(part);
    }

    public void addSubPart(String subPart) {
      subPartList.add(subPart);
    }

    public void setPartType(PartRefType type) {
      this.partType = type;
    }

    public void setErrorLimit(int errorLimit) {
      this.errorLimit = errorLimit;
    }

    public boolean isInsertPart() {
      return (!partList.isEmpty()) || (!subPartList.isEmpty());
    }
  }

  public static class tableSpec {
    public String dbName = null;
    public String tableName;
    public Table tableHandle;
    public HashMap<String, String> partSpec;
    public Partition partHandle;

    private InsertPartDesc insertPartDesc;

    public InsertPartDesc getInsertPartDesc() {
      return insertPartDesc;
    }

    public tableSpec() {

    }

    public void init(Hive db, HiveConf conf, ASTNode ast,
        Map<String, Table> tbls, Map<String, Boolean> tblFirstMap,
        int optimizeLevel) throws SemanticException {
      assert (ast.getToken().getType() == HiveParser.TOK_TAB);

      boolean isSuppInsertPart = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVEINSERTPARTSUPPORT);
      int mapErrorLimit = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVEINSERTPARTMAPERRORLIMIT);
      insertPartDesc = new InsertPartDesc();
      insertPartDesc.setErrorLimit(mapErrorLimit);
      tableHandle = null;

      String partName = null;
      String subPartName = null;
      PartRefType prt = null;
      String partColName = null;
      String subPartColName = null;
      String partValue = null;
      String subPartValue = null;

      int childIndex = 0;
      try {
        tableName = unescapeIdentifier(ast.getChild(0).getText());

        if (ast.getChildCount() >= 2) {
          switch (((ASTNode) ast.getChild(1)).getToken().getType()) {
          case HiveParser.TOK_PARTITIONREF:

            if (!isSuppInsertPart)
              throw new SemanticException(ast.getLine() + " line "
                  + ast.startIndex + "column:can not insert partition!");

            if (((ASTNode) ast.getChild(1).getChild(0)).getToken().getType() == HiveParser.TOK_PARTVAL) {
              ASTNode partValTree = (ASTNode) ast.getChild(1).getChild(0);
              if (partValTree.getChildCount() != 2) {
                throw new SemanticException(ast.getLine() + " line "
                    + partValTree.startIndex + ":partition expression error");
              }

              partColName = DDLSemanticAnalyzer
                  .getPartitionDefineValue((ASTNode) partValTree.getChild(0)
                      .getChild(0));
              partValue = DDLSemanticAnalyzer
                  .getPartitionDefineValue((ASTNode) partValTree.getChild(1));
            } else {
              partName = unescapeIdentifier(
                  ast.getChild(1).getChild(0).getText()).toLowerCase();
              prt = QB.PartRefType.PRI;
              insertPartDesc.setPartType(prt);
              insertPartDesc.addPart(partName);
            }

            break;

          case HiveParser.TOK_SUBPARTITIONREF:

            if (!isSuppInsertPart)
              throw new SemanticException(ast.getLine() + " line "
                  + ast.startIndex + ":can not insert partition!");

            subPartName = unescapeIdentifier(
                ast.getChild(1).getChild(0).getText()).toLowerCase();
            prt = QB.PartRefType.SUB;
            insertPartDesc.setPartType(prt);
            insertPartDesc.addSubPart(subPartName);

            break;

          case HiveParser.TOK_COMPPARTITIONREF:

            if (!isSuppInsertPart)
              throw new SemanticException(ast.getLine() + " line "
                  + ast.startIndex + ":can not insert partition!");

            partName = unescapeIdentifier(ast.getChild(1).getChild(0).getText())
                .toLowerCase();
            subPartName = unescapeIdentifier(
                ast.getChild(1).getChild(1).getText()).toLowerCase();
            prt = QB.PartRefType.COMP;

            insertPartDesc.setPartType(prt);
            insertPartDesc.addSubPart(subPartName);
            insertPartDesc.addPart(partName);

            break;

          default:
            dbName = unescapeIdentifier(ast.getChild(1).getText())
                .toLowerCase();
            break;
          }

          if (ast.getChildCount() == 3) {
            dbName = unescapeIdentifier(ast.getChild(2).getText())
                .toLowerCase();
          }
        }

        if (dbName == null) {
          dbName = SessionState.get().getDbName();
        }

        boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
        if (testMode) {
          tableName = conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX)
              + tableName;
        }

        tableHandle = BaseSemanticAnalyzer.getTableMeta(db, dbName, tableName,
            tbls, tblFirstMap, optimizeLevel);

        if (partName != null) {
          if (tableHandle.isView())
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex + ":can not use partition with view!");

          if (!tableHandle.isPartitioned())
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex
                + ":can not use partition with a non-partition table!");

          if (!tableHandle.getTTable().getPriPartition().getParSpaces()
              .containsKey(partName))
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex + ":wrong pri-partition name: " + partName);
        }

        if (partColName != null) {
          if (tableHandle.isView()) {
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex + ":can not use partition with view!");
          }

          if (!tableHandle.isPartitioned()) {
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex
                + ":can not use partition with a non-partition table!");
          }

          if ("range".equalsIgnoreCase(tableHandle.getTTable()
              .getPriPartition().getParType())) {
            throw new SemanticException(
                "range partition can not support insert with partition values");
          }

          FieldSchema partField = tableHandle.getTTable().getPriPartition()
              .getParKey();
          if (!partField.getName().equalsIgnoreCase(partColName)) {
            throw new SemanticException(partColName
                + " is not a partition column");
          }

          PrimitiveTypeInfo pti = new PrimitiveTypeInfo();

          pti.setTypeName(partField.getType());

          ObjectInspector StringIO = PrimitiveObjectInspectorFactory
              .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);

          ObjectInspector ValueIO = PrimitiveObjectInspectorFactory
              .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());

          ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters
              .getConverter(StringIO, ValueIO);

          ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters
              .getConverter(StringIO, ValueIO);

          Map<String, List<String>> partSpaces = tableHandle.getTTable()
              .getPriPartition().getParSpaces();
          String guessPartName = "p_" + partValue;
          List<String> partValueList = partSpaces.get(guessPartName);
          boolean partFound = false;
          int ret = -1;
          int vSize = 0;

          if (partValueList != null && !partValueList.isEmpty()) {
            vSize = partValueList.size();
            for (int i = 0; i < vSize; i++) {
              ret = ((Comparable) converter1.convert(partValue))
                  .compareTo((Comparable) converter2.convert(partValueList
                      .get(i)));
              if (ret == 0) {
                prt = QB.PartRefType.PRI;
                insertPartDesc.setPartType(prt);
                insertPartDesc.addPart(guessPartName);
                partFound = true;
                break;
              }
            }

            if (!partFound) {
              throw new SemanticException("partition " + guessPartName
                  + "already exist, but not contain value:" + partValue);
            }
          }

          if (!partFound) {
            for (Entry<String, List<String>> entry : partSpaces.entrySet()) {
              partValueList = entry.getValue();
              if (partValueList != null) {
                vSize = partValueList.size();
                for (int i = 0; i < vSize; i++) {
                  ret = ((Comparable) converter1.convert(partValue))
                      .compareTo((Comparable) converter2.convert(partValueList
                          .get(i)));
                  if (ret == 0) {
                    prt = QB.PartRefType.PRI;
                    insertPartDesc.setPartType(prt);
                    insertPartDesc.addPart(entry.getKey());
                    partFound = true;
                    break;
                  }
                }
              }

              if (partFound) {
                break;
              }
            }
          }

          if (!partFound) {
            PartitionDesc pd = new PartitionDesc();
            pd.setIsSub(false);
            pd.setPartitionType(PartitionType.LIST_PARTITION);
            pd.setPartColumn(partField);

            if (!validateName(guessPartName)) {
              throw new SemanticException(guessPartName
                  + " is not a valid partition name, maybe you should add"
                  + " partition youself first");
            }

            LinkedHashMap<String, List<String>> addPartSpaces = new LinkedHashMap<String, List<String>>();
            List<String> addPartValueList = new ArrayList<String>();
            addPartValueList.add(partValue);
            addPartSpaces.put(guessPartName, addPartValueList);
            pd.setPartitionSpaces(addPartSpaces);

            AddPartitionDesc apd = new AddPartitionDesc(dbName, tableName, pd,
                false);
            apd.setPartDesc(pd);

            try {
              db.addPartitions(apd);
            } catch (Exception e) {
              throw new SemanticException("add partition " + guessPartName
                  + " error, msg=" + e.getMessage());
            }

            partSpaces.put(guessPartName, addPartValueList);
            prt = QB.PartRefType.PRI;
            insertPartDesc.setPartType(prt);
            insertPartDesc.addPart(guessPartName);
          }
        }

        if (subPartName != null) {
          if (tableHandle.isView())
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex + ":can not use partition with view!");

          if (!tableHandle.isPartitioned())
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex
                + ":can not use partition with a non-partition table!");

          if (!tableHandle.getTTable().getSubPartition().getParSpaces()
              .containsKey(subPartName))
            throw new SemanticException(ast.getLine() + " line "
                + ast.startIndex + ":wrong sub-partition name: " + subPartName);
        }
      } catch (InvalidTableException ite) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast
            .getChild(0)), ite);
      } catch (HiveException e) {
        e.printStackTrace();
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(
            ast.getChild(childIndex), e.getMessage()), e);
      }
    }

    public String toString() {
      if (partHandle != null)
        return partHandle.toString();
      else
        return tableHandle.toString();
    }

    public void init() {

    }

  }

  static public boolean validateName(String name) {
    Pattern tpat = Pattern.compile("[\\w_]+");
    Matcher m = tpat.matcher(name);
    if (m.matches()) {
      return true;
    }

    return false;
  }

  public String getDbnameFromAST(ASTNode ast) {
    String dbName = null;
    if (ast.getChildCount() >= 2) {
      switch (((ASTNode) ast.getChild(1)).getToken().getType()) {
      case HiveParser.TOK_PARTITIONREF:
      case HiveParser.TOK_SUBPARTITIONREF:
      case HiveParser.TOK_COMPPARTITIONREF:
        break;

      default:
        dbName = unescapeIdentifier(ast.getChild(1).getText()).toLowerCase();
        break;
      }

      if (ast.getChildCount() == 3) {
        dbName = unescapeIdentifier(ast.getChild(2).getText()).toLowerCase();
      }
    }

    if (dbName == null)
      dbName = SessionState.get().getDbName().toLowerCase();
    return dbName;
  }
}
