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
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NGramEstimator {
  /* Class private variables */
  private int k;
  private int pf;
  private int n;
  private HashMap<ArrayList<String>, Double> ngrams;

  public NGramEstimator() {
    k = 0;
    pf = 0;
    n = 0;
    ngrams = new HashMap<ArrayList<String>, Double>();
  }

  public boolean isInitialized() {
    return (k != 0);
  }

  public void initialize(int pk, int ppf, int pn) throws HiveException {
    assert (pk > 0 && ppf > 0 && pn > 0);
    k = pk;
    pf = ppf;
    n = pn;

    if (k * pf < 1000) {
      pf = 1000 / k;
    }
  }

  public void reset() {
    ngrams.clear();
    n = pf = k = 0;
  }

  public ArrayList<Object[]> getNGrams() throws HiveException {
    trim(true);
    if (ngrams.size() < 1) {
      return null;
    }

    ArrayList<Object[]> result = new ArrayList<Object[]>();
    ArrayList<Map.Entry<ArrayList<String>, Double>> list = new ArrayList(
        ngrams.entrySet());
    Collections.sort(list,
        new Comparator<Map.Entry<ArrayList<String>, Double>>() {
          public int compare(Map.Entry<ArrayList<String>, Double> o1,
              Map.Entry<ArrayList<String>, Double> o2) {
            return o2.getValue().compareTo(o1.getValue());
          }
        });

    for (int i = 0; i < list.size(); i++) {
      ArrayList<String> key = list.get(i).getKey();
      Double val = list.get(i).getValue();

      Object[] curGram = new Object[2];
      ArrayList<Text> ng = new ArrayList<Text>();
      for (int j = 0; j < key.size(); j++) {
        ng.add(new Text(key.get(j)));
      }
      curGram[0] = ng;
      curGram[1] = new DoubleWritable(val.doubleValue());
      result.add(curGram);
    }

    return result;
  }

  public int size() {
    return ngrams.size();
  }

  public void add(ArrayList<String> ng) throws HiveException {
    assert (ng != null && ng.size() > 0 && ng.get(0) != null);
    Double curFreq = ngrams.get(ng);
    if (curFreq == null) {
      curFreq = new Double(1.0);
    } else {
      curFreq++;
    }
    ngrams.put(ng, curFreq);

    if (n == 0) {
      n = ng.size();
    } else {
      if (n != ng.size()) {
        throw new HiveException(getClass().getSimpleName()
            + ": mismatch in value for 'n'"
            + ", which usually is caused by a non-constant expression. Found '"
            + n + "' and '" + ng.size() + "'.");
      }
    }

    if (ngrams.size() > k * pf * 2) {
      trim(false);
    }
  }

  private void trim(boolean finalTrim) throws HiveException {
    ArrayList<Map.Entry<ArrayList<String>, Double>> list = new ArrayList(
        ngrams.entrySet());
    Collections.sort(list,
        new Comparator<Map.Entry<ArrayList<String>, Double>>() {
          public int compare(Map.Entry<ArrayList<String>, Double> o1,
              Map.Entry<ArrayList<String>, Double> o2) {
            return o1.getValue().compareTo(o2.getValue());
          }
        });
    for (int i = 0; i < list.size() - (finalTrim ? k : pf * k); i++) {
      ngrams.remove(list.get(i).getKey());
    }
  }

  public void merge(List<org.apache.hadoop.io.Text> other) throws HiveException {
    if (other == null) {
      return;
    }

    int otherK = Integer.parseInt(other.get(0).toString());
    int otherN = Integer.parseInt(other.get(1).toString());
    int otherPF = Integer.parseInt(other.get(2).toString());
    if (k > 0 && k != otherK) {
      throw new HiveException(getClass().getSimpleName()
          + ": mismatch in value for 'k'"
          + ", which usually is caused by a non-constant expression. Found '"
          + k + "' and '" + otherK + "'.");
    }
    if (n > 0 && otherN != n) {
      throw new HiveException(getClass().getSimpleName()
          + ": mismatch in value for 'n'"
          + ", which usually is caused by a non-constant expression. Found '"
          + n + "' and '" + otherN + "'.");
    }
    if (pf > 0 && otherPF != pf) {
      throw new HiveException(getClass().getSimpleName()
          + ": mismatch in value for 'pf'"
          + ", which usually is caused by a non-constant expression. Found '"
          + pf + "' and '" + otherPF + "'.");
    }
    k = otherK;
    pf = otherPF;
    n = otherN;

    for (int i = 3; i < other.size(); i++) {
      ArrayList<String> key = new ArrayList<String>();
      for (int j = 0; j < n; j++) {
        Text word = other.get(i + j);
        key.add(word.toString());
      }
      i += n;
      double val = Double.parseDouble(other.get(i).toString());
      Double myval = ngrams.get(key);
      if (myval == null) {
        myval = new Double(val);
      } else {
        myval += val;
      }
      ngrams.put(key, myval);
    }

    trim(false);
  }

  public ArrayList<Text> serialize() throws HiveException {
    ArrayList<Text> result = new ArrayList<Text>();
    result.add(new Text(Integer.toString(k)));
    result.add(new Text(Integer.toString(n)));
    result.add(new Text(Integer.toString(pf)));
    for (Iterator<ArrayList<String>> it = ngrams.keySet().iterator(); it
        .hasNext();) {
      ArrayList<String> mykey = it.next();
      assert (mykey.size() > 0);
      for (int i = 0; i < mykey.size(); i++) {
        result.add(new Text(mykey.get(i)));
      }
      Double myval = ngrams.get(mykey);
      result.add(new Text(myval.toString()));
    }

    return result;
  }
}
