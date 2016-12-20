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

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.parse.PartitionType;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.thrift.protocol.TBinaryProtocol;

public class TestHive extends TestCase {
  private Hive hm;
  private HiveConf hiveConf;
  private FileSystem fs;

  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    fs = FileSystem.get(hiveConf);
    try {
      this.hm = Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to initialize Hive Metastore using configruation: \n "
              + hiveConf);
      throw e;
    }
  }

  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      Hive.closeCurrent();
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to close Hive Metastore using configruation: \n "
              + hiveConf);
      throw e;
    }
  }

  public void testTable() throws Throwable {
    try {
      SessionState.start(hiveConf);
      String tableName = "table_for_testtable";
      try {
        this.hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        e1.printStackTrace();
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(tableName);
      List<FieldSchema> fields = tbl.getCols();

      FieldSchema partCol1 = new FieldSchema(
          "ds",
          Constants.STRING_TYPE_NAME,
          "partition column, date but in string format as date type is not yet supported in QL");

      FieldSchema partCol2 = new FieldSchema(
          "gender",
          Constants.STRING_TYPE_NAME,
          "partition column, date but in string format as date type is not yet supported in QL");

      fields.add(new FieldSchema("col1", Constants.INT_TYPE_NAME,
          "int -- first column"));
      fields.add(new FieldSchema("col2", Constants.STRING_TYPE_NAME,
          "string -- second column"));
      fields.add(new FieldSchema("col3", Constants.DOUBLE_TYPE_NAME,
          "double -- thrift column"));
      fields.add(partCol1);
      fields.add(partCol2);

      tbl.setFields(fields);

      LinkedHashMap<String, List<String>> ps1 = new LinkedHashMap<String, List<String>>();

      LinkedHashMap<String, List<String>> ps2 = new LinkedHashMap<String, List<String>>();

      ArrayList<String> pardef_less10 = new ArrayList<String>();
      pardef_less10.add("10");

      ArrayList<String> pardef_less20 = new ArrayList<String>();
      pardef_less20.add("20");

      ps1.put("p1_less10", pardef_less10);

      ps1.put("p2_less20", pardef_less20);

      ArrayList<String> boy = new ArrayList<String>();

      boy.add("boy");
      boy.add("man");
      boy.add("m");

      ArrayList<String> girl = new ArrayList<String>();

      girl.add("girl");
      girl.add("woman");
      girl.add("f");

      ps2.put("sp1_boy", boy);
      ps2.put("sp2_girl", girl);

      PartitionDesc SubPartdesc = new PartitionDesc(
          PartitionType.LIST_PARTITION, partCol2, ps2, null);
      SubPartdesc.setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
      SubPartdesc.setTableName("table_for_testtable");

      PartitionDesc PriPartdesc = new PartitionDesc(
          PartitionType.RANGE_PARTITION, partCol1, ps1, SubPartdesc);
      PriPartdesc.setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
      PriPartdesc.setTableName("table_for_testtable");

      tbl.setPartitions(PriPartdesc);

      tbl.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      tbl.setProperty("comment",
          "this is a test table created as part junit tests");

      List<String> bucketCols = tbl.getBucketCols();
      bucketCols.add("col1");
      try {
        tbl.setBucketCols(bucketCols);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to set bucket column for table: " + tableName, false);
      }

      tbl.setNumBuckets((short) 512);
      tbl.setOwner("allison");
      tbl.setRetention(10);

      tbl.setSerdeParam(Constants.FIELD_DELIM, "1");
      tbl.setSerdeParam(Constants.LINE_DELIM, "\n");
      tbl.setSerdeParam(Constants.MAPKEY_DELIM, "3");
      tbl.setSerdeParam(Constants.COLLECTION_DELIM, "2");

      tbl.setSerdeParam(Constants.FIELD_DELIM, "1");
      tbl.setSerializationLib(LazySimpleSerDe.class.getName());

      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      Table ft = null;
      Warehouse wh = new Warehouse(hiveConf);
      try {
        ft = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
        ft.checkValidity();
        assertEquals("Table names didn't match for table: " + tableName,
            tbl.getName(), ft.getName());
        assertEquals("Table owners didn't match for table: " + tableName,
            tbl.getOwner(), ft.getOwner());
        assertEquals("Table retention didn't match for table: " + tableName,
            tbl.getRetention(), ft.getRetention());
        assertEquals(
            "Data location is not set correctly",
            wh.getDefaultTablePath(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                tableName).toString(), ft.getDataLocation().toString());
        tbl.setDataLocation(ft.getDataLocation());

        assertEquals("Serde is not set correctly", tbl.getDeserializer()
            .getClass().getName(), ft.getDeserializer().getClass().getName());
        assertEquals("SerializationLib is not set correctly",
            tbl.getSerializationLib(), LazySimpleSerDe.class.getName());

        assertEquals("the pri partition type RANGE", tbl.getTTable()
            .getPriPartition().getParType().toLowerCase(), ft.getTTable()
            .getPriPartition().getParType().toLowerCase());
        assertEquals("the sub partition type LIST", tbl.getTTable()
            .getSubPartition().getParType().toLowerCase(), ft.getTTable()
            .getSubPartition().getParType().toLowerCase());

        assertEquals("the pri partition size", ft.getTTable().getPriPartition()
            .getParSpaces().size(), 2);

        assertEquals("the sub partition size", ft.getTTable().getSubPartition()
            .getParSpaces().size(), 2);

        assertEquals("the pri partition p1 less than value size",
            ft.getTTable().getPriPartition().getParSpaces().get("p1_less10")
                .get(0), "10");

        assertEquals("the pri partition p2 less than value size",
            ft.getTTable().getPriPartition().getParSpaces().get("p2_less20")
                .get(0), "20");

        assertEquals(
            "the sub partition sp1 list values index 1",
            ft.getTTable().getSubPartition().getParSpaces().get("sp1_boy")
                .get(0), "boy");

        assertEquals("the sub partition sp2 list values index 3", ft
            .getTTable().getSubPartition().getParSpaces().get("sp2_girl")
            .get(2), "f");

      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to fetch table correctly: " + tableName, false);
      }

      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, true,
            false);
        Table ft2 = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
            tableName, false);
        assertNull("Unable to drop table ", ft2);
      } catch (HiveException e) {
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTable failed");
      throw e;
    }
  }

  public void testThriftTable() throws Throwable {
    SessionState.start(hiveConf);
    String tableName = "table_for_test_thrifttable";
    try {
      try {
        this.hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        System.err.println(StringUtils.stringifyException(e1));
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(tableName);
      tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
      tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
      tbl.setSerializationLib(ThriftDeserializer.class.getName());
      tbl.setSerdeParam(Constants.SERIALIZATION_CLASS, Complex.class.getName());
      tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT,
          TBinaryProtocol.class.getName());
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      Warehouse wh = new Warehouse(hiveConf);
      Table ft = null;
      try {
        ft = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
        assertNotNull("Unable to fetch table", ft);
        ft.checkValidity();
        assertEquals("Table names didn't match for table: " + tableName,
            tbl.getName(), ft.getName());
        assertEquals("Table owners didn't match for table: " + tableName,
            tbl.getOwner(), ft.getOwner());
        assertEquals("Table retention didn't match for table: " + tableName,
            tbl.getRetention(), ft.getRetention());
        assertEquals(
            "Data location is not set correctly",
            wh.getDefaultTablePath(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                tableName).toString(), ft.getDataLocation().toString());
        tbl.setDataLocation(ft.getDataLocation());
        assertTrue("Tables  doesn't match: " + tableName, ft.getTTable()
            .equals(tbl.getTTable()));
        assertEquals("SerializationLib is not set correctly",
            tbl.getSerializationLib(), ThriftDeserializer.class.getName());
        assertEquals("Serde is not set correctly", tbl.getDeserializer()
            .getClass().getName(), ft.getDeserializer().getClass().getName());
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch table correctly: " + tableName, false);
      }
      hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testThriftTable() failed");
      throw e;
    }
  }

  private static Table createTestTable(String dbName, String tableName)
      throws HiveException {
    Table tbl = new Table(tableName);
    tbl.getTTable().setDbName(dbName);
    tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
    tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
    tbl.setSerializationLib(ThriftDeserializer.class.getName());
    tbl.setSerdeParam(Constants.SERIALIZATION_CLASS, Complex.class.getName());
    tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT,
        TBinaryProtocol.class.getName());
    return tbl;
  }

}
