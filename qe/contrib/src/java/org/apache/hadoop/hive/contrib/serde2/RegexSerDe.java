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
package org.apache.hadoop.hive.contrib.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.MissingFormatArgumentException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class RegexSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(RegexSerDe.class.getName());
  
  int numColumns;
  String inputRegex;
  String outputFormatString;
  
  Pattern inputPattern;
  
  StructObjectInspector rowOI;
  ArrayList<String> row;
  
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    
    
    inputRegex = tbl.getProperty("input.regex");
    outputFormatString = tbl.getProperty("output.format.string");
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    boolean inputRegexIgnoreCase = 
      "true".equalsIgnoreCase(tbl.getProperty("input.regex.case.insensitive"));

    if (inputRegex != null) {
      inputPattern = Pattern.compile(inputRegex, Pattern.DOTALL
          + (inputRegexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    } else {
      inputPattern = null;
    }
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size(); 
    
    for (int c = 0; c < numColumns; c++) {
      if (!columnTypes.get(c).equals(TypeInfoFactory.stringTypeInfo)) {
        throw new SerDeException(getClass().getName() 
            + " only accepts string columns, but column[" + c 
            + "] named " + columnNames.get(c) + " has type "
            + columnTypes.get(c));
      }
    }
    
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    
    row = new ArrayList<String>(numColumns);
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
    outputFields = new Object[numColumns];
    outputRowText = new Text();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  long unmatchedRows = 0;
  long nextUnmatchedRows = 1;
  long partialMatchedRows = 0;
  long nextPartialMatchedRows = 1;

  long getNextNumberToDisplay(long now) {
    return now*10;
  }
  
  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    if (inputPattern == null) {
      throw new SerDeException("This table does not have serde property \"input.regex\"!");
    }
    Text rowText = (Text)blob;
    
    Matcher m = inputPattern.matcher(rowText.toString());
    
    if (!m.matches()) {
      unmatchedRows ++;
      if (unmatchedRows >= nextUnmatchedRows) {
        nextUnmatchedRows = getNextNumberToDisplay(nextUnmatchedRows);
        LOG.warn("" + unmatchedRows + " unmatched rows are found: " 
            + rowText);
      }
      return null;
    }
    
    for (int c = 0; c < numColumns; c++) {
      try {
        row.set(c, m.group(c + 1));
      } catch (RuntimeException e) {
        partialMatchedRows ++;
        if (partialMatchedRows >= nextPartialMatchedRows) {
          nextPartialMatchedRows = getNextNumberToDisplay(nextPartialMatchedRows);
          LOG.warn("" + partialMatchedRows + " partially unmatched rows are found, "
              + " cannot find group " + c + ": " + rowText);
        }
        row.set(c, null); 
      }
    }
    return row;
  }

  Object[] outputFields;
  Text outputRowText;
  
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    
    if (outputFormatString == null) {
      throw new SerDeException("Cannot write data into table because \"output.format.string\""
          + " is not specified in serde properties of the table.");
    }
    
    
    StructObjectInspector outputRowOI = (StructObjectInspector)objInspector;
    List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();
    if (outputFieldRefs.size() != numColumns) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has " + numColumns +
          " columns.");
    }
    
    for (int c = 0; c < numColumns; c++) {
      Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
      ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();
      StringObjectInspector fieldStringOI = (StringObjectInspector)fieldOI;
      outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field); 
    }
    
    String outputRowString = null;
    try {
      outputRowString = String.format(outputFormatString, outputFields);
    } catch (MissingFormatArgumentException e) {
      throw new SerDeException("The table contains " + numColumns 
          + " columns, but the outputFormatString is asking for more.", e);
    }
    outputRowText.set(outputRowString);
    return outputRowText;
  }
  
}
