/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GenericUDAFWm_concat extends AbstractGenericUDAFResolver {
  @description(name = "wm_concat", value = "_FUNC_(x,[,splitstr[,'asc'[,y]]]) - Returns the cancat of x")
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
      return new GenericUDAFWm_concatEvaluator();
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }

  }

  public static class GenericUDAFWm_concatEvaluator extends
      GenericUDAFEvaluator {

    PrimitiveObjectInspector inputOI1;
    PrimitiveObjectInspector inputOI2;
    PrimitiveObjectInspector inputOI3;
    PrimitiveObjectInspector inputOI4;

    StructObjectInspector soi;
    StructField concatvaluesfields;
    StructField sortvaluesfields;
    StructField sortmodefield;
    StructField splitchfield;
    StructField sortbyselffield;

    ListObjectInspector cvOI1;
    ListObjectInspector svOI2;
    IntObjectInspector smOI3;
    StringObjectInspector scOI4;
    BooleanObjectInspector sbsOI5;

    Object[] partialResult;

    Text result;

    public GenericUDAFWm_concatEvaluator() {
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI1 = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1)
          inputOI2 = (PrimitiveObjectInspector) parameters[1];
        if (parameters.length > 2)
          inputOI3 = (PrimitiveObjectInspector) parameters[2];
        if (parameters.length > 3)
          inputOI4 = (PrimitiveObjectInspector) parameters[3];
      } else {
        soi = (StructObjectInspector) parameters[0];

        concatvaluesfields = soi.getStructFieldRef("cancatvalues");
        sortvaluesfields = soi.getStructFieldRef("sortvalues");
        sortmodefield = soi.getStructFieldRef("sortmode");
        splitchfield = soi.getStructFieldRef("splitch");
        sortbyselffield = soi.getStructFieldRef("sortbyself");

        cvOI1 = (ListObjectInspector) concatvaluesfields
            .getFieldObjectInspector();
        svOI2 = (ListObjectInspector) sortvaluesfields
            .getFieldObjectInspector();
        smOI3 = (IntObjectInspector) sortmodefield.getFieldObjectInspector();
        scOI4 = (StringObjectInspector) splitchfield.getFieldObjectInspector();
        sbsOI5 = (BooleanObjectInspector) sortbyselffield
            .getFieldObjectInspector();
      }

      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(ObjectInspectorFactory
            .getStandardListObjectInspector(ObjectInspectorUtils
                .getStandardObjectInspector(parameters[0])));
        foi.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils
            .getStandardObjectInspector(parameters.length > 3 ? parameters[3]
                : parameters[0])));
        foi.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("cancatvalues");
        fname.add("sortvalues");
        fname.add("sortmode");
        fname.add("splitch");
        fname.add("sortbyself");

        partialResult = new Object[5];
        partialResult[0] = new ArrayList<Object>();
        partialResult[1] = new ArrayList<Object>();
        partialResult[2] = new IntWritable(0);
        partialResult[3] = new Text();
        partialResult[4] = new BooleanWritable(true);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);

      } else {
        result = new Text();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      }
    }

    static class Wm_concatAgg implements AggregationBuffer {
      ArrayList<Object> concatvalues = new ArrayList<Object>();
      ArrayList<Object> sortvalues = new ArrayList<Object>();
      int sortmode = 0;
      String splitch = null;
      boolean sortbyselfcolumn = true;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      Wm_concatAgg result = new Wm_concatAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      Wm_concatAgg myagg = (Wm_concatAgg) agg;
      myagg.concatvalues.clear();
      myagg.sortvalues.clear();
      myagg.splitch = null;
      myagg.sortmode = 0;
      myagg.sortbyselfcolumn = true;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      Wm_concatAgg myagg = (Wm_concatAgg) agg;
      Object p = parameters[0];
      myagg.concatvalues.add(ObjectInspectorUtils.copyToStandardObject(p,
          inputOI1));
      if (parameters.length > 1) {
        p = parameters[1];
        if (p != null) {
          String v = ((Text) ObjectInspectorUtils.copyToStandardObject(p,
              inputOI2)).toString();
          if (myagg.splitch == null) {
            myagg.splitch = v;
          } else if (!myagg.splitch.equals(v)) {
            throw new HiveException("split char error.....");
          }
        }
      }
      if (parameters.length > 2) {
        p = parameters[2];
        if (p != null) {
          String v = ((Text) ObjectInspectorUtils.copyToStandardObject(p,
              inputOI3)).toString();
          if (myagg.sortmode == 0) {
            if (v.equalsIgnoreCase("asc")) {
              myagg.sortmode = 1;
            } else if (v.equalsIgnoreCase("desc")) {
              myagg.sortmode = -1;
            } else {
              throw new HiveException("sort mode must be asc or desc");
            }
          } else {
            if ((v.equalsIgnoreCase("asc") && myagg.sortmode == -1)
                || (v.equalsIgnoreCase("desc") && myagg.sortmode == 1)) {
              throw new HiveException("sort mode error.....");
            } else if (!v.equalsIgnoreCase("asc")
                && !v.equalsIgnoreCase("desc")) {
              throw new HiveException("sort mode must be asc or desc");
            }
          }
        }
      }
      if (parameters.length > 3) {
        p = parameters[3];
        myagg.sortvalues.add(ObjectInspectorUtils.copyToStandardObject(p,
            inputOI4));
        myagg.sortbyselfcolumn = false;
      }

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      Wm_concatAgg myagg = (Wm_concatAgg) agg;

      ((ArrayList<Object>) partialResult[0]).clear();
      ((ArrayList<Object>) partialResult[1]).clear();
      ((ArrayList<Object>) partialResult[0]).addAll(myagg.concatvalues);
      ((ArrayList<Object>) partialResult[1]).addAll(myagg.sortvalues);
      ((IntWritable) partialResult[2]).set(myagg.sortmode);
      ((Text) partialResult[3]).set(myagg.splitch == null ? "" : myagg.splitch);
      ((BooleanWritable) partialResult[4]).set(myagg.sortbyselfcolumn);

      return partialResult;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        Wm_concatAgg myagg = (Wm_concatAgg) agg;

        Object obj1 = soi.getStructFieldData(partial, concatvaluesfields);
        Object obj2 = soi.getStructFieldData(partial, sortvaluesfields);
        Object obj3 = soi.getStructFieldData(partial, sortmodefield);
        Object obj4 = soi.getStructFieldData(partial, splitchfield);
        Object obj5 = soi.getStructFieldData(partial, sortbyselffield);

        List<Object> str1 = (List<Object>) ObjectInspectorUtils
            .copyToStandardObject(obj1, cvOI1);
        List<Object> str2 = (List<Object>) ObjectInspectorUtils
            .copyToStandardObject(obj2, svOI2);
        int sm = ((IntWritable) ObjectInspectorUtils.copyToStandardObject(obj3,
            smOI3)).get();
        String str4 = ((Text) ObjectInspectorUtils.copyToStandardObject(obj4,
            scOI4)).toString();
        Boolean bflag = ((BooleanWritable) ObjectInspectorUtils
            .copyToStandardObject(obj5, sbsOI5)).get();

        if (str1 == null) {
          str1 = new ArrayList<Object>();
          str1.add(null);
        }
        if (str2 == null) {
          str2 = new ArrayList<Object>();
          str2.add(null);
        }

        if (str1.size() != str2.size()) {
          if (str1.size() < str2.size()) {
            int s = str2.size() - str1.size();
            for (int i = 0; i < s; i++) {
              str1.add(null);
            }
          } else {
            int s = str1.size() - str2.size();
            for (int i = 0; i < s; i++) {
              str2.add(null);
            }
          }
        }

        myagg.concatvalues.addAll(str1);
        myagg.sortvalues.addAll(str2);
        myagg.sortmode = sm;
        myagg.splitch = str4;
        myagg.sortbyselfcolumn = bflag;

      }

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      Wm_concatAgg myagg = (Wm_concatAgg) agg;

      StringBuffer sb = new StringBuffer();
      String sch = myagg.splitch == null ? "" : myagg.splitch;
      if (myagg.sortmode == 0) {
        for (Object str : myagg.concatvalues) {
          if (str != null)
            sb.append(str).append(sch);
        }
        sb.setLength(sb.length() > 0 ? sb.length() - sch.length() : 0);
      } else {
        Comparator<ConcatValuePair> c;
        ObjectInspector oi = null;

        if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
          if (myagg.sortbyselfcolumn) {
            oi = inputOI1;
          } else {
            oi = inputOI4;
          }
        } else {
          if (myagg.sortbyselfcolumn) {
            oi = cvOI1.getListElementObjectInspector();
          } else {
            oi = svOI2.getListElementObjectInspector();
          }
        }
        final ObjectInspector oi1 = ObjectInspectorUtils
            .getStandardObjectInspector(oi);

        if (myagg.sortmode > 0) {
          c = new Comparator<ConcatValuePair>() {
            @Override
            public int compare(ConcatValuePair o1, ConcatValuePair o2) {
              return ObjectInspectorUtils.compare(o1.sortvalue, oi1,
                  o2.sortvalue, oi1);
            }
          };
        } else {
          c = new Comparator<ConcatValuePair>() {

            @Override
            public int compare(ConcatValuePair o1, ConcatValuePair o2) {
              return -1
                  * ObjectInspectorUtils.compare(o1.sortvalue, oi1,
                      o2.sortvalue, oi1);
            }
          };
        }
        MinHeap<ConcatValuePair> heap = new MinHeap<ConcatValuePair>(c);

        if (!myagg.sortbyselfcolumn) {
          if (myagg.sortvalues == null || myagg.sortvalues.size() == 0) {
            result.set("");
            return result;
          }

          for (int i = 0; i < myagg.sortvalues.size(); i++) {
            heap.add(new ConcatValuePair(myagg.concatvalues.get(i),
                myagg.sortvalues.get(i)));
          }
        } else {
          if (myagg.concatvalues == null || myagg.concatvalues.size() == 0) {
            result.set("");
            return result;
          }
          for (int i = 0; i < myagg.concatvalues.size(); i++) {
            heap.add(new ConcatValuePair(myagg.concatvalues.get(i),
                myagg.concatvalues.get(i)));
          }
        }

        sb.setLength(0);
        while (!heap.isEmpty()) {
          Object o = heap.pop().concatvalue;
          if (o != null)
            sb.append(o).append(sch);
        }
        sb.setLength(sb.length() > 0 ? sb.length() - sch.length() : 0);
      }
      String sbstr = sb.toString();

      if (sbstr.length() >= 4 * 1024 * 1024) {
        throw new HiveException("the result size is too long :\t"
            + sbstr.length());
      }

      result.set(sbstr);
      return result;
    }
  }

  static class ConcatValuePair {
    Object concatvalue;
    Object sortvalue;

    public ConcatValuePair(Object concatvalue, Object sortvalue) {
      this.concatvalue = concatvalue;
      this.sortvalue = sortvalue;
    }
  }
}
