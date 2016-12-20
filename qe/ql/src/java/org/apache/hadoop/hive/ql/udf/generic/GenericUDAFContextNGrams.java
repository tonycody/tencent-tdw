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

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;

@description(name = "context_ngrams", value = "_FUNC_(expr, array<string1, string2, ...>, k, pf) estimates the top-k most "
    + "frequent n-grams that fit into the specified context. The second parameter specifies "
    + "a string of words that specify the positions of the n-gram elements, with a null value "
    + "standing in for a 'blank' that must be filled by an n-gram element.", extended = "The primary expression must be an array of strings, or an array of arrays of "
    + "strings, such as the return type of the sentences() UDF. The second parameter specifies "
    + "the context -- for example, array(\"i\", \"love\", null) -- which would estimate the top "
    + "'k' words that follow the phrase \"i love\" in the primary expression. The optional "
    + "fourth parameter 'pf' controls the memory used by the heuristic. Larger values will "
    + "yield better accuracy, but use more memory. Example usage:\n"
    + "  SELECT context_ngrams(sentences(lower(review)), array(\"i\", \"love\", null, null), 10)"
    + " FROM movies\n"
    + "would attempt to determine the 10 most common two-word phrases that follow \"i love\" "
    + "in a database of free-form natural language movie reviews.")
public class GenericUDAFContextNGrams implements GenericUDAFResolver {
  static final Log LOG = LogFactory.getLog(GenericUDAFContextNGrams.class
      .getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 3 && parameters.length != 4) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Please specify either three or four arguments.");
    }

    PrimitiveTypeInfo pti;
    if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentTypeException(0,
          "Only list type arguments are accepted but "
              + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    switch (((ListTypeInfo) parameters[0]).getListElementTypeInfo()
        .getCategory()) {
    case PRIMITIVE:
      pti = (PrimitiveTypeInfo) ((ListTypeInfo) parameters[0])
          .getListElementTypeInfo();
      break;

    case LIST:
      ListTypeInfo lti = (ListTypeInfo) ((ListTypeInfo) parameters[0])
          .getListElementTypeInfo();
      pti = (PrimitiveTypeInfo) lti.getListElementTypeInfo();
      break;

    default:
      throw new UDFArgumentTypeException(0,
          "Only arrays of strings or arrays of arrays of strings are accepted but "
              + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    if (pti.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(0,
          "Only array<string> or array<array<string>> is allowed, but "
              + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    if (parameters[1].getCategory() != ObjectInspector.Category.LIST
        || ((ListTypeInfo) parameters[1]).getListElementTypeInfo()
            .getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "Only arrays of strings are accepted but "
              + parameters[1].getTypeName() + " was passed as parameter 2.");
    }
    if (((PrimitiveTypeInfo) ((ListTypeInfo) parameters[1])
        .getListElementTypeInfo()).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(1,
          "Only arrays of strings are accepted but "
              + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(2, "Only integers are accepted but "
          + parameters[2].getTypeName() + " was passed as parameter 3.");
    }
    switch (((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case TIMESTAMP:
      break;

    default:
      throw new UDFArgumentTypeException(2, "Only integers are accepted but "
          + parameters[2].getTypeName() + " was passed as parameter 3.");
    }

    if (parameters.length == 4) {
      if (parameters[3].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(3, "Only integers are accepted but "
            + parameters[3].getTypeName() + " was passed as parameter 4.");
      }
      switch (((PrimitiveTypeInfo) parameters[3]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case TIMESTAMP:
        break;

      default:
        throw new UDFArgumentTypeException(3, "Only integers are accepted but "
            + parameters[3].getTypeName() + " was passed as parameter 4.");
      }
    }

    return new GenericUDAFContextNGramEvaluator();
  }

  public static class GenericUDAFContextNGramEvaluator extends
      GenericUDAFEvaluator {
    private StandardListObjectInspector outerInputOI;
    private StandardListObjectInspector innerInputOI;
    private StandardListObjectInspector contextListOI;
    private PrimitiveObjectInspector contextOI;
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector kOI;
    private PrimitiveObjectInspector pOI;

    private ListObjectInspector loi;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        outerInputOI = (StandardListObjectInspector) parameters[0];
        if (outerInputOI.getListElementObjectInspector().getCategory() == ObjectInspector.Category.LIST) {
          innerInputOI = (StandardListObjectInspector) outerInputOI
              .getListElementObjectInspector();
          inputOI = (PrimitiveObjectInspector) innerInputOI
              .getListElementObjectInspector();
        } else {
          inputOI = (PrimitiveObjectInspector) outerInputOI
              .getListElementObjectInspector();
          innerInputOI = null;
        }
        contextListOI = (StandardListObjectInspector) parameters[1];
        contextOI = (PrimitiveObjectInspector) contextListOI
            .getListElementObjectInspector();
        kOI = (PrimitiveObjectInspector) parameters[2];
        if (parameters.length == 4) {
          pOI = (PrimitiveObjectInspector) parameters[3];
        } else {
          pOI = null;
        }
      } else {
        loi = (ListObjectInspector) parameters[0];
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        return ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      } else {
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector));
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("ngram");
        fname.add("estfrequency");
        return ObjectInspectorFactory
            .getStandardListObjectInspector(ObjectInspectorFactory
                .getStandardStructObjectInspector(fname, foi));
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object obj) throws HiveException {
      if (obj == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;

      Object slo = ObjectInspectorUtils.copyToStandardObject(obj, loi);

      StandardListObjectInspector sloi = (StandardListObjectInspector) ObjectInspectorUtils
          .getStandardObjectInspector(loi);

      List<Text> partial = (List<Text>) sloi.getList(slo);

      int contextSize = Integer
          .parseInt(((Text) partial.get(partial.size() - 1)).toString());
      partial.remove(partial.size() - 1);
      if (myagg.context.size() > 0) {
        if (contextSize != myagg.context.size()) {
          throw new HiveException(
              getClass().getSimpleName()
                  + ": found a mismatch in the"
                  + " context string lengths. This is usually caused by passing a non-constant"
                  + " expression for the context.");
        }
      } else {
        for (int i = partial.size() - contextSize; i < partial.size(); i++) {
          String word = partial.get(i).toString();
          if (word.equals("")) {
            myagg.context.add(null);
          } else {
            myagg.context.add(word);
          }
        }
        partial.subList(partial.size() - contextSize, partial.size()).clear();
        myagg.nge.merge(partial);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;
      ArrayList<Text> result = myagg.nge.serialize();

      for (int i = 0; i < myagg.context.size(); i++) {
        if (myagg.context.get(i) == null) {
          result.add(new Text(""));
        } else {
          result.add(new Text(myagg.context.get(i)));
        }
      }
      result.add(new Text(Integer.toString(myagg.context.size())));

      return result;
    }

    private void processNgrams(NGramAggBuf agg, ArrayList<String> seq)
        throws HiveException {
      assert (agg.context.size() > 0);
      ArrayList<String> ng = new ArrayList<String>();
      for (int i = seq.size() - agg.context.size(); i >= 0; i--) {
        boolean contextMatches = true;
        ng.clear();
        for (int j = 0; j < agg.context.size(); j++) {
          String contextWord = agg.context.get(j);
          if (contextWord == null) {
            ng.add(seq.get(i + j));
          } else {
            if (!contextWord.equals(seq.get(i + j))) {
              contextMatches = false;
              break;
            }
          }
        }

        if (contextMatches) {
          agg.nge.add(ng);
          ng = new ArrayList<String>();
        }
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 3 || parameters.length == 4);
      if (parameters[0] == null || parameters[1] == null
          || parameters[2] == null) {
        return;
      }
      NGramAggBuf myagg = (NGramAggBuf) agg;

      if (!myagg.nge.isInitialized()) {
        int k = PrimitiveObjectInspectorUtils.getInt(parameters[2], kOI);
        int pf = 0;
        if (k < 1) {
          throw new HiveException(getClass().getSimpleName()
              + " needs 'k' to be at least 1, " + "but you supplied " + k);
        }
        if (parameters.length == 4) {
          pf = PrimitiveObjectInspectorUtils.getInt(parameters[3], pOI);
          if (pf < 1) {
            throw new HiveException(getClass().getSimpleName()
                + " needs 'pf' to be at least 1, " + "but you supplied " + pf);
          }
        } else {
          pf = 1;
        }

        myagg.context.clear();
        List<Text> context = (List<Text>) contextListOI.getList(parameters[1]);
        int contextNulls = 0;
        for (int i = 0; i < context.size(); i++) {
          String word = PrimitiveObjectInspectorUtils.getString(context.get(i),
              contextOI);
          if (word == null) {
            contextNulls++;
          }
          myagg.context.add(word);
        }
        if (context.size() == 0) {
          throw new HiveException(getClass().getSimpleName()
              + " needs a context array " + "with at least one element.");
        }
        if (contextNulls == 0) {
          throw new HiveException(
              getClass().getSimpleName()
                  + " the context array needs to "
                  + "contain at least one 'null' value to indicate what should be counted.");
        }

        myagg.nge.initialize(k, pf, contextNulls);
      }

      List<Text> outer = (List<Text>) outerInputOI.getList(parameters[0]);
      if (innerInputOI != null) {
        for (int i = 0; i < outer.size(); i++) {
          List<Text> inner = (List<Text>) innerInputOI.getList(outer.get(i));
          ArrayList<String> words = new ArrayList<String>();
          for (int j = 0; j < inner.size(); j++) {
            String word = PrimitiveObjectInspectorUtils.getString(inner.get(j),
                inputOI);
            words.add(word);
          }

          processNgrams(myagg, words);
        }
      } else {
        ArrayList<String> words = new ArrayList<String>();
        for (int i = 0; i < outer.size(); i++) {
          String word = PrimitiveObjectInspectorUtils.getString(outer.get(i),
              inputOI);
          words.add(word);
        }

        processNgrams(myagg, words);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      NGramAggBuf myagg = (NGramAggBuf) agg;
      return myagg.nge.getNGrams();
    }

    static class NGramAggBuf implements AggregationBuffer {
      ArrayList<String> context;
      NGramEstimator nge;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      NGramAggBuf result = new NGramAggBuf();
      result.nge = new NGramEstimator();
      result.context = new ArrayList<String>();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      NGramAggBuf result = (NGramAggBuf) agg;
      result.context.clear();
      result.nge.reset();
    }
  }
}
