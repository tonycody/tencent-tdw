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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

@description(name = "explode", value = "_FUNC_(a) - separates the elements of array a into multiple rows ")
public class GenericUDTFExplode extends GenericUDTF {
    
  public static boolean isChangSizeZero2Null = false;
  public static boolean isChangNull2Null = false;

  ListObjectInspector listOI = null;
  boolean needToConvert = false;
  ObjectInspectorConverters.Converter myconvert = null;

  @Override
  public void close() throws HiveException {
  }

  private boolean checkCategory(ObjectInspector inputOI,
      ObjectInspector outputOI, boolean inList) throws UDFArgumentException {
    boolean ret = false;

    ObjectInspector inputOIElement = null;
    ObjectInspector outputOIElement = null;

    if (outputOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      if (inputOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        PrimitiveObjectInspector inputOIpoi = (PrimitiveObjectInspector) inputOI;
        PrimitiveObjectInspector outputOIpoi = (PrimitiveObjectInspector) outputOI;

        if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
            throw new UDFArgumentException(
                "explode column category and default value category don't match");
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BYTE) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BYTE) {
            throw new UDFArgumentException(
                "explode column category and default value category don't match");
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.SHORT) {
            throw new UDFArgumentException(
                "explode column category and default value category don't match");
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentException(
                "explode column category and default value category don't match");
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            if (inputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT
                && !inList) {
              return true;
            } else {
              throw new UDFArgumentException(
                  "explode column category and default value category don't match");
            }
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
            if (inputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
                && !inList) {
              return true;
            } else {
              throw new UDFArgumentException(
                  "explode column category and default value category don't match");
            }
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            if (inputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT
                && !inList) {
              return true;
            } else {
              throw new UDFArgumentException(
                  "explode column category and default value category don't match");
            }
          }
        } else if (outputOIpoi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
          if (inputOIpoi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException(
                "explode column category and default value category don't match");
          }
        } else {
          ;
        }
      } else {
        throw new UDFArgumentException(
            "explode column category and default value category don't match");
      }
    } else if (outputOI.getCategory() == ObjectInspector.Category.LIST) {
      if (inputOI.getCategory() == ObjectInspector.Category.LIST) {
        inputOIElement = ((ListObjectInspector) inputOI)
            .getListElementObjectInspector();
        outputOIElement = ((ListObjectInspector) outputOI)
            .getListElementObjectInspector();
        ret = checkCategory(inputOIElement, outputOIElement, true);
      } else {
        throw new UDFArgumentException(
            "explode column category and default value category don't match");
      }
    } else {
      throw new UDFArgumentException(
          "explode default value cannot support Map and Struct Category");
    }

    return ret;
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {

    if (args.length > 2) {
      throw new UDFArgumentException(
          "Only explode(col) or explode(col, default_value) support in explode");
    }

    if (args[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("explode() takes an array as a parameter");
    }

    boolean getStandard = args.length == 2 ? true : false;

    if (getStandard)
      listOI = (ListObjectInspector) ObjectInspectorUtils
          .getStandardObjectInspector(args[0],
              ObjectInspectorCopyOption.WRITABLE);
    else
      listOI = (ListObjectInspector) args[0];

    ObjectInspector defaultOI = null;
    ObjectInspector listElementOI = null;
    if (args.length == 2) {
      defaultOI = args[1];
      listElementOI = listOI.getListElementObjectInspector();
      needToConvert = checkCategory(defaultOI, listElementOI, false);
      if (needToConvert)
        myconvert = ObjectInspectorConverters.getConverter(defaultOI,
            listElementOI);
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col");
    fieldOIs.add(listOI.getListElementObjectInspector());
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  Object forwardObj[] = new Object[1];

  @Override
  public void process(Object[] o) throws HiveException {

    List<?> list = listOI.getList(o[0]);
    if (list == null) {
      if (isChangNull2Null) {
        forwardObj[0] = null;
        this.forward(forwardObj);
      }
      return;
    }

    if (list.size() == 0 && o.length == 2 && o[1] != null) {
      if (needToConvert)
        o[1] = myconvert.convert(o[1]);
      forwardObj[0] = o[1];
      this.forward(forwardObj);
    } else if (list.size() == 0 && isChangSizeZero2Null) {
      forwardObj[0] = null;
      this.forward(forwardObj);
    }

    for (int i = 0; i < list.size(); i++) {
      forwardObj[0] = list.get(i);
      this.forward(forwardObj);
    }

  }

  @Override
  public String toString() {
    return "explode";
  }
}
