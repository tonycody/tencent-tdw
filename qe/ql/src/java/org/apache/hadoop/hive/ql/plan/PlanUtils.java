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

package org.apache.hadoop.hive.ql.plan;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;

import org.apache.thrift.protocol.TBinaryProtocol;

public class PlanUtils {

  public static enum ExpressionTypes {
    FIELD, JEXL
  };

  @SuppressWarnings("nls")
  public static mapredWork getMapRedWork() {
    return new mapredWork("", new LinkedHashMap<String, ArrayList<String>>(),
        new LinkedHashMap<String, partitionDesc>(),
        new LinkedHashMap<String, Operator<? extends Serializable>>(),
        new tableDesc(), new ArrayList<tableDesc>(), null, Integer.valueOf(1),
        null);
  }

  public static tableDesc getDefaultTableDesc(String separatorCode,
      String columns) {
    return getDefaultTableDesc(separatorCode, columns, false);
  }

  public static tableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns) {
    return getTableDesc(serdeClass, separatorCode, columns, false);
  }

  public static tableDesc getDefaultTableDesc(String separatorCode,
      String columns, boolean lastColumnTakesRestOfTheLine) {
    return getDefaultTableDesc(separatorCode, columns, null,
        lastColumnTakesRestOfTheLine);
  }

  public static tableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(serdeClass, separatorCode, columns, null,
        lastColumnTakesRestOfTheLine);
  }

  public static tableDesc getDefaultTableDesc(String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(LazySimpleSerDe.class, separatorCode, columns,
        columnTypes, lastColumnTakesRestOfTheLine);
  }

  public static tableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(serdeClass, separatorCode, columns, columnTypes,
        lastColumnTakesRestOfTheLine, false);
  }

  public static tableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine,
      boolean useJSONForLazy) {

    Properties properties = Utilities.makeProperties(
        Constants.SERIALIZATION_FORMAT, separatorCode, Constants.LIST_COLUMNS,
        columns);
    if (columnTypes != null)
      properties.setProperty(Constants.LIST_COLUMN_TYPES, columnTypes);

    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }

    if (useJSONForLazy)
      properties.setProperty(Constants.SERIALIZATION_USE_JSON_OBJECTS, "true");

    return new tableDesc(serdeClass, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, properties);
  }

  /* add by roachxiang for ctas begin */

  public static tableDesc getTableDesc(createTableDesc crtTblDesc, String cols,
      String colTypes) {

    Class<? extends Deserializer> serdeClass = LazySimpleSerDe.class;
    String separatorCode = Integer.toString(Utilities.ctrlaCode);
    String columns = cols;
    String columnTypes = colTypes;
    tableDesc ret;
    boolean lastColumnTakesRestOfTheLine = false;

    try {
      if (crtTblDesc.getSerName() != null) {
        Class c = Class.forName(crtTblDesc.getSerName());
        serdeClass = c;
      }

      ret = getTableDesc(serdeClass, separatorCode, columns, columnTypes,
          lastColumnTakesRestOfTheLine, false);

      Class c1 = Class.forName(crtTblDesc.getInputFormat());
      Class c2 = Class.forName(crtTblDesc.getOutputFormat());
      Class<? extends InputFormat> in_class = c1;
      Class<? extends HiveOutputFormat> out_class = c2;

      ret.setInputFileFormatClass(in_class);
      ret.setOutputFileFormatClass(out_class);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
    return ret;
  }

  /* add by roachxiang for ctas end */

  public static tableDesc getDefaultTableDesc(String separatorCode) {
    return new tableDesc(MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class, IgnoreKeyTextOutputFormat.class,
        Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
            separatorCode));
  }

  public static tableDesc getReduceKeyTableDesc(List<FieldSchema> fieldSchemas,
      String order) {
    return new tableDesc(BinarySortableSerDe.class,
        SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        Utilities.makeProperties(Constants.LIST_COLUMNS,
            MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES,
            MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.SERIALIZATION_SORT_ORDER, order));
  }

  public static tableDesc getMapJoinKeyTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties("columns",
            MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            "columns.types",
            MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"));
  }

  public static tableDesc getMapJoinValueTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new tableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties("columns",
            MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            "columns.types",
            MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"));
  }

  public static tableDesc getIntermediateFileTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new tableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
            Constants.LIST_COLUMNS,
            MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES,
            MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"));
  }

  public static tableDesc getReduceValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
            Constants.LIST_COLUMNS,
            MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES,
            MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"));
  }

  public static List<FieldSchema> getFieldSchemasFromColumnListWithLength(
      List<exprNodeDesc> cols, List<List<Integer>> distinctColIndices,
      List<String> outputColumnNames, int length, String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(length + 1);
    for (int i = 0; i < length; i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix
          + outputColumnNames.get(i), cols.get(i).getTypeInfo()));
    }

    List<TypeInfo> unionTypes = new ArrayList<TypeInfo>();
    for (List<Integer> distinctCols : distinctColIndices) {
      List<String> names = new ArrayList<String>();
      List<TypeInfo> types = new ArrayList<TypeInfo>();
      int numExprs = 0;
      for (int i : distinctCols) {
        names.add(HiveConf.getColumnInternalName(numExprs));
        types.add(cols.get(i).getTypeInfo());
        numExprs++;
      }
      unionTypes.add(TypeInfoFactory.getStructTypeInfo(names, types));
    }
    if (!unionTypes.isEmpty()) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix
          + outputColumnNames.get(length),
          TypeInfoFactory.getUnionTypeInfo(unionTypes)));
    }

    return schemas;
  }

  public static List<FieldSchema> getFieldSchemasFromColumnList(
      List<exprNodeDesc> cols, List<String> outputColumnNames, int start,
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix
          + outputColumnNames.get(i + start), cols.get(i).getTypeInfo()));
    }
    return schemas;
  }

  public static List<FieldSchema> getFieldSchemasFromColumnList(
      List<exprNodeDesc> cols, String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix + i,
          cols.get(i).getTypeInfo()));
    }
    return schemas;
  }

  public static List<FieldSchema> getFieldSchemasFromRowSchema(RowSchema row,
      String fieldPrefix) {
    Vector<ColumnInfo> c = row.getSignature();
    return getFieldSchemasFromColumnInfo(c, fieldPrefix);
  }

  public static List<FieldSchema> getFieldSchemasFromColumnInfo(
      Vector<ColumnInfo> cols, String fieldPrefix) {
    if ((cols == null) || (cols.size() == 0))
      return new ArrayList<FieldSchema>();

    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      String name = cols.get(i).getInternalName();
      if (name.equals(Integer.valueOf(i).toString())) {
        name = fieldPrefix + name;
      }
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(name, cols.get(i)
          .getType()));
    }
    return schemas;
  }

  public static List<FieldSchema> sortFieldSchemas(List<FieldSchema> schema) {
    Collections.sort(schema, new Comparator<FieldSchema>() {

      @Override
      public int compare(FieldSchema o1, FieldSchema o2) {
        return o1.getName().compareTo(o2.getName());
      }

    });
    return schema;
  }

  public static reduceSinkDesc getReduceSinkDesc(
      ArrayList<exprNodeDesc> keyCols, ArrayList<exprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKeyCols, int tag,
      ArrayList<exprNodeDesc> partitionCols, String order, int numReducers) {
    return getReduceSinkDesc(
        keyCols,
        keyCols.size(),
        valueCols,
        new ArrayList<List<Integer>>(),
        includeKeyCols ? outputColumnNames.subList(0, keyCols.size())
            : new ArrayList<String>(),
        includeKeyCols ? outputColumnNames.subList(keyCols.size(),
            outputColumnNames.size()) : outputColumnNames, includeKeyCols, tag,
        partitionCols, order, numReducers, false);
  }

  public static reduceSinkDesc getReduceSinkDesc(
      final ArrayList<exprNodeDesc> keyCols, int numKeys,
      ArrayList<exprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      boolean includeKeyCols, int tag, ArrayList<exprNodeDesc> partitionCols,
      String order, int numReducers, boolean partitionByGbkeyanddistinctkey) {
    tableDesc keyTable = null;
    tableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    if (includeKeyCols) {
      keyTable = getReduceKeyTableDesc(
          getFieldSchemasFromColumnListWithLength(keyCols, distinctColIndices,
              outputKeyColumnNames, numKeys, ""), order);
      outputKeyCols.addAll(outputKeyColumnNames);
    } else {
      keyTable = getReduceKeyTableDesc(
          getFieldSchemasFromColumnList(keyCols, "reducesinkkey"), order);
      for (int i = 0; i < keyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey" + i);
      }
    }
    valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
        valueCols, outputValueColumnNames, 0, ""));
    outputValCols.addAll(outputValueColumnNames);
    return new reduceSinkDesc(keyCols, numKeys, valueCols, outputKeyCols,
        distinctColIndices, outputValCols, tag, partitionCols, numReducers,
        keyTable, valueTable, partitionByGbkeyanddistinctkey);
  }

  public static reduceSinkDesc getReduceSinkDesc(
      ArrayList<exprNodeDesc> keyCols, ArrayList<exprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKey, int tag,
      int numPartitionFields, int numReducers) throws SemanticException {
    return getReduceSinkDesc(
        keyCols,
        keyCols.size(),
        valueCols,
        new ArrayList<List<Integer>>(),
        includeKey ? outputColumnNames.subList(0, keyCols.size())
            : new ArrayList<String>(),
        includeKey ? outputColumnNames.subList(keyCols.size(),
            outputColumnNames.size()) : outputColumnNames, includeKey, tag,
        numPartitionFields, numReducers, false);
  }

  public static reduceSinkDesc getReduceSinkDesc(
      ArrayList<exprNodeDesc> keyCols, int numKeys,
      ArrayList<exprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      boolean includeKey, int tag, int numPartitionFields, int numReducers,
      boolean partitionByGbkeyanddistinctkey) throws SemanticException {
    ArrayList<exprNodeDesc> partitionCols = null;

    if (numPartitionFields >= keyCols.size()) {
      partitionCols = keyCols;
    } else if (numPartitionFields >= 0) {
      partitionCols = new ArrayList<exprNodeDesc>(numPartitionFields);
      for (int i = 0; i < numPartitionFields; i++) {
        partitionCols.add(keyCols.get(i));
      }
    } else {
      partitionCols = new ArrayList<exprNodeDesc>(1);
      partitionCols.add(TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("rand"));
    }

    StringBuilder order = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      order.append("+");
    }
    return getReduceSinkDesc(keyCols, numKeys, valueCols, distinctColIndices,
        outputKeyColumnNames, outputValueColumnNames, includeKey, tag,
        partitionCols, order.toString(), numReducers,
        partitionByGbkeyanddistinctkey);
  }

  public static halfSortLimitDesc getHalfSortLimitDesc(
      ArrayList<exprNodeDesc> sortCols, String order, int limit) {
    return new halfSortLimitDesc(sortCols, order, limit);
  }

  public static reduceSinkDesc getReduceSinkDesc1(
      ArrayList<exprNodeDesc> reduceKeys, ArrayList<exprNodeDesc> reduceValues,
      ArrayList<ArrayList<Integer>> distTag2AggrPos,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      int tag, int numReducers, boolean mapAggrDone,
      boolean outputdistinctaggrparam, boolean partitionByGbkeyanddistinctkey,
      boolean outputvalues, exprNodeDesc aggrPartExpr,
      ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr) {

    tableDesc keyTable = null;
    tableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    StringBuilder order = new StringBuilder();
    for (int i = 0; i < reduceKeys.size() + 1; i++) {
      order.append("+");
    }

    keyTable = getReduceKeyTableDesc(
        getFieldSchemasFromColumnList1(reduceKeys, aggrPartExpr,
            outputKeyColumnNames), order.toString());
    outputKeyCols.addAll(outputKeyColumnNames);
    valueTable = getReduceValueTableDesc(new ArrayList<FieldSchema>());

    return new reduceSinkDesc(reduceKeys, reduceValues, outputKeyCols,
        outputValCols, tag, numReducers, mapAggrDone, keyTable, valueTable,
        outputdistinctaggrparam, partitionByGbkeyanddistinctkey, outputvalues,
        aggrPartExpr);
  }

  public static List<FieldSchema> getFieldSchemasFromColumnList1(
      List<exprNodeDesc> cols, exprNodeDesc aggrPartExpr,
      List<String> outputColumnNames) {
    int length = cols.size();
    List<FieldSchema> schemas = new ArrayList<FieldSchema>();
    for (int i = 0; i < length; i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputColumnNames.get(i), cols.get(i).getTypeInfo()));
    }

    if (aggrPartExpr != null) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputColumnNames.get(cols.size()), aggrPartExpr.getTypeInfo()));
    }

    return schemas;
  }

  public static List<FieldSchema> getFieldSchemasFromColumnList1(
      exprNodeDesc unionReduceValuesExpr, List<String> outputColumnNames) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>();

    if (unionReduceValuesExpr != null) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputColumnNames.get(0), unionReduceValuesExpr.getTypeInfo()));
    }
    return schemas;
  }

  public static reduceSinkDesc getReduceSinkDesc2(
      ArrayList<exprNodeDesc> reduceKeys, ArrayList<exprNodeDesc> reduceValues,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      int tag, int numReducers, boolean mapAggrDone, boolean isFirstReduce,
      boolean partitionByGbkeyanddistinctkey,
      ArrayList<ArrayList<Integer>> tag2AggrPos, exprNodeDesc aggrPartExpr,
      ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr,
      TypeInfo aggrPartTypeInfo) {

    tableDesc keyTable = null;
    tableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    StringBuilder order = new StringBuilder();
    for (int i = 0; i < (isFirstReduce ? reduceKeys.size() + 1 : reduceKeys
        .size()); i++) {
      order.append("+");
    }

    keyTable = getReduceKeyTableDesc(
        getFieldSchemasFromColumnList2(reduceKeys, isFirstReduce,
            aggrPartTypeInfo, outputKeyColumnNames), order.toString());
    outputKeyCols.addAll(outputKeyColumnNames);
    valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList2(
        isFirstReduce, aggrPartTypeInfo, outputValueColumnNames));
    outputValCols.addAll(outputValueColumnNames);

    return new reduceSinkDesc(reduceKeys, reduceValues, outputKeyCols,
        outputValCols, tag, numReducers, keyTable, valueTable, mapAggrDone,
        partitionByGbkeyanddistinctkey, isFirstReduce, aggrPartExpr,
        tag2AggrParamORValueExpr);
  }

  private static List<FieldSchema> getFieldSchemasFromColumnList2(
      boolean isFirstReduce, TypeInfo aggrPartTypeInfo,
      List<String> outputValueColumnNames) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>();
    if (!isFirstReduce && aggrPartTypeInfo != null) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputValueColumnNames.get(0), aggrPartTypeInfo));
    }

    return schemas;
  }

  private static List<FieldSchema> getFieldSchemasFromColumnList2(
      ArrayList<exprNodeDesc> reduceKeys, boolean isFirstReduce,
      TypeInfo aggrPartTypeInfo, List<String> outputKeyColumnNames) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>();
    for (int i = 0; i < reduceKeys.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputKeyColumnNames.get(i), reduceKeys.get(i).getTypeInfo()));
    }

    if (isFirstReduce && aggrPartTypeInfo != null) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
          outputKeyColumnNames.get(reduceKeys.size()), aggrPartTypeInfo));
    }

    return schemas;
  }

}
