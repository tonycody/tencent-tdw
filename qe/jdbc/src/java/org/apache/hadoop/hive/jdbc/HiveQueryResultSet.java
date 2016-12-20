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

package org.apache.hadoop.hive.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.PropertyConfigurator;

public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Log LOG = LogFactory.getLog(HiveQueryResultSet.class);

  private HiveInterface client;
  private SerDe serde;

  private int maxRows = 0;
  private int rowsFetched = 0;
  private int fetchSize = 50;

  private List<String> fetchedRows;
  private Iterator<String> fetchedRowsItr;

  public HiveQueryResultSet(HiveInterface client, int maxRows)
      throws SQLException {
    this.client = client;
    this.maxRows = maxRows;
    initSerde();
    row = Arrays.asList(new Object[columnNames.size()]);

  }

  public HiveQueryResultSet(HiveInterface client) throws SQLException {
    this(client, 0);

  }

  private void initSerde() throws SQLException {
    try {

      Schema fullSchema = client.getSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if ((schema != null) && (!schema.isEmpty())) {
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          columnNames.add(schema.get(pos).getName());
          columnTypes.add(schema.get(pos).getType());
          namesSb.append(schema.get(pos).getName());
          typesSb.append(schema.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        props.setProperty(Constants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        props.setProperty(Constants.LIST_COLUMN_TYPES, types);
      }
      serde.initialize(new Configuration(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(),
          ex);
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      client.clean();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean next() throws SQLException {
    if (maxRows > 0 && rowsFetched >= maxRows) {
      return false;
    }

    try {
      if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
        fetchedRows = client.fetchN(fetchSize);
        if (fetchedRows == null || fetchedRows.size() == 0) {
          client.clean();
          return false;
        }

        fetchedRowsItr = fetchedRows.iterator();
      }

      String rowStr = "";
      if (fetchedRowsItr.hasNext()) {
        rowStr = fetchedRowsItr.next();
      } else {
        return false;
      }

      rowsFetched++;

      StructObjectInspector soi = (StructObjectInspector) serde
          .getObjectInspector();
      List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
      Object data = serde.deserialize(new BytesWritable(rowStr
          .getBytes("UTF-8")));

      assert row.size() == fieldRefs.size() : row.size() + ", "
          + fieldRefs.size();
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        ObjectInspector oi = fieldRef.getFieldObjectInspector();
        Object obj = soi.getStructFieldData(data, fieldRef);

        row.set(i, convertLazyToJava(obj, oi));
      }

    } catch (HiveServerException e) {
      if (e.getErrorCode() == 0) {
        return false;
      } else {
        throw new SQLException("Error retrieving next row", e);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row", ex);
    }
    return true;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  private static Object convertLazyToJava(Object o, ObjectInspector oi) {
    Object obj = ObjectInspectorUtils.copyToStandardObject(o, oi,
        ObjectInspectorCopyOption.JAVA);

    if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      obj = obj.toString();
    }

    return obj;
  }
}
