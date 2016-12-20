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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;

public class FetchTask extends Task<fetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private byte[] DefaultSeparators = { (byte) 1 };
  private String mysep = "	";

  private int maxRows = 100;
  private FetchOperator ftOp;
  private SerDe mSerde;
  private int totalRows;

  public void initialize(HiveConf conf, DriverContext ctx) {
    
	  if(conf == null)
	  {
		  conf= new HiveConf();
	  }
	  
	  super.initialize(conf, ctx);

    try {
      JobConf job = new JobConf(conf, ExecDriver.class);

      Properties mSerdeProp = new Properties();
      String serdeName = HiveConf.getVar(conf,
          HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);

      if (serdeName.length() != 0) {
        Class<? extends SerDe> serdeClass = Class.forName(serdeName, true,
            JavaUtils.getClassLoader()).asSubclass(SerDe.class);
        mSerde = (SerDe) ReflectionUtils.newInstance(serdeClass, null);
        mSerde.initialize(job, mSerdeProp);

        mysep = new String(DefaultSeparators);
      } else {

        mSerde = new LazySimpleSerDe();

        mSerdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
        mSerdeProp.put(Constants.SERIALIZATION_NULL_FORMAT,
            ((fetchWork) work).getSerializationNullFormat());
        mSerde.initialize(job, mSerdeProp);
        ((LazySimpleSerDe) mSerde).setUseJSONSerialize(true);
      }

      ftOp = new FetchOperator(work, job);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  public int execute() {
    assert false;
    return 0;
  }

  public tableDesc getTblDesc() {
    return work.getTblDesc();
  }

  public int getMaxRows() {
    return maxRows;
  }

  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean fetch(Vector<String> res) throws IOException {
    try {
      int numRows = 0;
      int rowsRet = maxRows;
      if ((work.getLimit() >= 0) && ((work.getLimit() - totalRows) < rowsRet))
        rowsRet = work.getLimit() - totalRows;
      if (rowsRet <= 0) {
        ftOp.clearFetchContext();
        return false;
      }

      Vector<Integer> vvv = new Vector<Integer>();
      int countcols = work.getTblDesc().getColsnum();
      if (work.getTblDesc().getIsSelectOp()) {
        vvv = work.getTblDesc().getSelectCols();
        if (vvv.size() == 0) {
          throw new Exception("there are no cols selected!");
        }
      }

      while (numRows < rowsRet) {
        InspectableObject io = ftOp.getNextRow();

        if (io == null) {
          if (numRows == 0)
            return false;
          totalRows += numRows;
          return true;
        }

        String getrow = ((Text) mSerde.serialize(io.o, io.oi)).toString();
        if (work.getTblDesc().getIsSelectOp()) {
          String[] tmpArr;
          if (mysep.equals("	")) {
            tmpArr = getrow.split("\\t");
          } else {
            tmpArr = getrow.split(mysep);
          }

          if (tmpArr.length > countcols) {
            throw new Exception("there is at least one col which contains tab!");
          }
          String newstr = "";
          Iterator<Integer> it = vvv.iterator();
          while (it.hasNext()) {
            int index = it.next().intValue();
            if (index < tmpArr.length) {
              newstr += (tmpArr[index] + mysep);
            } else {
              newstr += mysep;
            }
          }
          getrow = newstr;
        }
        res.add(getrow);

        numRows++;
      }
      totalRows += numRows;

      return true;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "FETCH";
  }
}
