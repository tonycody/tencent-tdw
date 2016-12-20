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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.explain;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public abstract class Operator<T extends Serializable> implements Serializable,
    Node {

  private static final long serialVersionUID = 1L;

  protected List<Operator<? extends Serializable>> childOperators;
  protected List<Operator<? extends Serializable>> parentOperators;
  private static int seqId;

  public static enum State {
    UNINIT, INIT, CLOSE
  };

  transient protected State state = State.UNINIT;

  static {
    seqId = 0;
  }

  public Operator() {
    id = String.valueOf(seqId++);
  }

  public Operator(Reporter reporter) {
    this.reporter = reporter;
    id = String.valueOf(seqId++);
  }

  public void setChildOperators(
      List<Operator<? extends Serializable>> childOperators) {
    this.childOperators = childOperators;
  }

  public List<Operator<? extends Serializable>> getChildOperators() {
    return childOperators;
  }

  public Vector<Node> getChildren() {

    if (getChildOperators() == null) {
      return null;
    }

    Vector<Node> ret_vec = new Vector<Node>();
    for (Operator<? extends Serializable> op : getChildOperators()) {
      ret_vec.add(op);
    }

    return ret_vec;
  }

  public void setParentOperators(
      List<Operator<? extends Serializable>> parentOperators) {
    this.parentOperators = parentOperators;
  }

  public List<Operator<? extends Serializable>> getParentOperators() {
    return parentOperators;
  }

  protected T conf;
  protected boolean done;

  public void setConf(T conf) {
    this.conf = conf;
  }

  @explain
  public T getConf() {
    return conf;
  }

  public boolean getDone() {
    return done;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  transient private RowSchema rowSchema;

  public void setSchema(RowSchema rowSchema) {
    this.rowSchema = rowSchema;
  }

  public RowSchema getSchema() {
    return rowSchema;
  }

  transient protected HashMap<Enum<?>, LongWritable> statsMap = new HashMap<Enum<?>, LongWritable>();
  transient protected OutputCollector out;
  transient protected Log LOG = LogFactory.getLog(this.getClass().getName());
  transient protected String alias;
  transient protected Reporter reporter;
  transient protected String id;
  transient protected ObjectInspector[] inputObjInspectors = new ObjectInspector[Byte.MAX_VALUE];
  transient protected ObjectInspector outputObjInspector;

  transient protected Object groupKeyObject;

  public void setGroupKeyObject(Object keyObject) {
    this.groupKeyObject = keyObject;
  }

  public Object getGroupKeyObject() {
    return groupKeyObject;
  }

  protected transient Map<String, exprNodeDesc> colExprMap;

  public void setId(String id) {
    this.id = id;
  }

  public String getIdentifier() {
    return id;
  }

  public void setReporter(Reporter rep) {
    reporter = rep;

    if (childOperators == null)
      return;

    for (Operator<? extends Serializable> op : childOperators) {
      op.setReporter(rep);
    }
  }

  public void setOutputCollector(OutputCollector out) {
    this.out = out;

    if (childOperators == null)
      return;

    for (Operator<? extends Serializable> op : childOperators) {
      op.setOutputCollector(out);
    }
  }

  public void setAlias(String alias) {
    this.alias = alias;

    if (childOperators == null)
      return;

    for (Operator<? extends Serializable> op : childOperators) {
      op.setAlias(alias);
    }
  }

  public Map<Enum<?>, Long> getStats() {
    HashMap<Enum<?>, Long> ret = new HashMap<Enum<?>, Long>();
    for (Enum<?> one : statsMap.keySet()) {
      ret.put(one, Long.valueOf(statsMap.get(one).get()));
    }
    return (ret);
  }

  protected boolean areAllParentsInitialized() {
    if (parentOperators == null) {
      return true;
    }
    for (Operator<? extends Serializable> parent : parentOperators) {
      if (parent.state != State.INIT) {
        return false;
      }
    }
    return true;
  }

  public void initialize(Configuration hconf, ObjectInspector[] inputOIs)
      throws HiveException {
    if (state == State.INIT) {
      return;
    }

    if (!areAllParentsInitialized()) {
      return;
    }
    LOG.info("Initializing Self " + id + " " + getName());

    if (inputOIs != null) {
      inputObjInspectors = inputOIs;
    }

    if (childOperators != null) {
      childOperatorsArray = new Operator[childOperators.size()];
      for (int i = 0; i < childOperatorsArray.length; i++) {
        childOperatorsArray[i] = childOperators.get(i);
      }
      childOperatorsTag = new int[childOperatorsArray.length];
      for (int i = 0; i < childOperatorsArray.length; i++) {
        List<Operator<? extends Serializable>> parentOperators = childOperatorsArray[i]
            .getParentOperators();
        if (parentOperators == null) {
          throw new HiveException("Hive internal error: parent is null in "
              + childOperatorsArray[i].getClass() + "!");
        }
        childOperatorsTag[i] = parentOperators.indexOf(this);
        if (childOperatorsTag[i] == -1) {
          throw new HiveException(
              "Hive internal error: cannot find parent in the child operator!");
        }
      }
    }

    if (inputObjInspectors.length == 0) {
      throw new HiveException("Internal Error during operator initialization.");
    }
    outputObjInspector = inputObjInspectors[0];
    initializeOp(hconf);
    LOG.info("Initialization Done " + id + " " + getName());
  }

  protected void initializeOp(Configuration hconf) throws HiveException {
    initializeChildren(hconf);
  }

  protected void initializeChildren(Configuration hconf) throws HiveException {
    state = State.INIT;
    LOG.info("Operator " + id + " " + getName() + " initialized");
    if (childOperators == null) {
      return;
    }
    LOG.info("Initializing children of " + id + " " + getName());
    for (int i = 0; i < childOperatorsArray.length; i++) {
      childOperatorsArray[i].initialize(hconf, outputObjInspector,
          childOperatorsTag[i]);
    }
  }

  private void initialize(Configuration hconf, ObjectInspector inputOI,
      int parentId) throws HiveException {
    LOG.info("Initializing child " + id + " " + getName());
    inputObjInspectors[parentId] = inputOI;
    initialize(hconf, null);
  }

  public abstract void process(Object row, int tag) throws HiveException;

  public void startGroup() throws HiveException {
    LOG.debug("Starting group");

    if (childOperators == null)
      return;

    LOG.debug("Starting group for children:");
    for (Operator<? extends Serializable> op : childOperators)
      op.startGroup();

    LOG.debug("Start group Done");
  }

  public void endGroup() throws HiveException {
    LOG.debug("Ending group");

    if (childOperators == null)
      return;

    LOG.debug("Ending group for children:");
    for (Operator<? extends Serializable> op : childOperators)
      op.endGroup();

    LOG.debug("End group Done");
  }

  private boolean allInitializedParentsAreClosed() {
    if (parentOperators != null) {
      for (Operator<? extends Serializable> parent : parentOperators) {
        if (!(parent.state == State.CLOSE || parent.state == State.UNINIT)) {
          return false;
        }
      }
    }
    return true;
  }

  public void close(boolean abort) throws HiveException {

    if (state == State.CLOSE)
      return;

    if (!allInitializedParentsAreClosed())
      return;

    state = State.CLOSE;
    LOG.info(id + " finished. closing... ");

    LOG.info(id + " forwarded " + cntr + " rows");

    closeOp(abort);

    try {
      logStats();
      if (childOperators == null)
        return;

      for (Operator<? extends Serializable> op : childOperators) {
        op.close(abort);
      }
      LOG.info(id + " Close done");
    } catch (HiveException e) {
      e.printStackTrace();
      throw e;
    }
  }

  protected void closeOp(boolean abort) throws HiveException {
  }

  public void jobClose(Configuration conf, boolean success)
      throws HiveException {
    if (childOperators == null)
      return;

    for (Operator<? extends Serializable> op : childOperators) {
      op.jobClose(conf, success);
    }
  }

  transient protected Operator<? extends Serializable>[] childOperatorsArray = null;
  transient protected int[] childOperatorsTag;

  transient private long cntr = 0;
  transient private long nextCntr = 1;

  public void replaceChild(Operator<? extends Serializable> child,
      Operator<? extends Serializable> newChild) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    childOperators.set(childIndex, newChild);
  }

  public void removeChild(Operator<? extends Serializable> child) {
    int childIndex = childOperators.indexOf(child);
    assert childIndex != -1;
    if (childOperators.size() == 1)
      childOperators = null;
    else
      childOperators.remove(childIndex);

    int parentIndex = child.getParentOperators().indexOf(this);
    assert parentIndex != -1;
    if (child.getParentOperators().size() == 1)
      child.setParentOperators(null);
    else
      child.getParentOperators().remove(parentIndex);
  }

  public void replaceParent(Operator<? extends Serializable> parent,
      Operator<? extends Serializable> newParent) {
    int parentIndex = parentOperators.indexOf(parent);
    assert parentIndex != -1;
    parentOperators.set(parentIndex, newParent);
  }

  private long getNextCntr(long cntr) {
    if (cntr >= 1000000)
      return cntr + 1000000;

    return 10 * cntr;
  }

  protected void forward(Object row, ObjectInspector rowInspector)
      throws HiveException {
    if (LOG.isInfoEnabled()) {
      cntr++;
      if (cntr == nextCntr) {
        LOG.info(id + " forwarding " + cntr + " rows");
        nextCntr = getNextCntr(cntr);
      }
    }

    if (childOperatorsArray == null && childOperators != null) {
      throw new HiveException(
          "Internal Hive error during operator initialization.");
    }

    if ((childOperatorsArray == null) || (getDone())) {
      return;
    }

    int childrenDone = 0;
    for (int i = 0; i < childOperatorsArray.length; i++) {
      Operator<? extends Serializable> o = childOperatorsArray[i];
      if (o.getDone()) {
        childrenDone++;
      } else {
        o.process(row, childOperatorsTag[i]);
      }
    }

    if (childrenDone == childOperatorsArray.length) {
      setDone(true);
    }
  }

  public void resetStats() {
    for (Enum<?> e : statsMap.keySet()) {
      statsMap.get(e).set(0L);
    }
  }

  public static interface OperatorFunc {
    public void func(Operator<? extends Serializable> op);
  }

  public void preorderMap(OperatorFunc opFunc) {
    opFunc.func(this);
    if (childOperators != null) {
      for (Operator<? extends Serializable> o : childOperators) {
        o.preorderMap(opFunc);
      }
    }
  }

  public void logStats() {
    for (Enum<?> e : statsMap.keySet()) {
      LOG.info(e.toString() + ":" + statsMap.get(e).toString());
    }
  }

  public String getName() {
    return new String("OP");
  }

  public Map<String, exprNodeDesc> getColumnExprMap() {
    return colExprMap;
  }

  public void setColumnExprMap(Map<String, exprNodeDesc> colExprMap) {
    this.colExprMap = colExprMap;
  }

  private String getLevelString(int level) {
    if (level == 0) {
      return "\n";
    }
    StringBuilder s = new StringBuilder();
    s.append("\n");
    while (level > 0) {
      s.append("  ");
      level--;
    }
    return s.toString();
  }

  public String dump(int level) {
    return dump(level, new HashSet<Integer>());
  }

  public String dump(int level, HashSet<Integer> seenOpts) {
    if (seenOpts.contains(new Integer(id)))
      return null;
    seenOpts.add(new Integer(id));

    StringBuilder s = new StringBuilder();
    String ls = getLevelString(level);
    s.append(ls);
    s.append("<" + getName() + ">");
    s.append("Id =" + id);
    if (childOperators != null) {
      s.append(ls);
      s.append("  <Children>");
      for (Operator<? extends Serializable> o : childOperators) {
        s.append(o.dump(level + 2, seenOpts));
      }
      s.append(ls);
      s.append("  <\\Children>");
    }

    if (parentOperators != null) {
      s.append(ls);
      s.append("  <Parent>");
      for (Operator<? extends Serializable> o : parentOperators) {
        s.append("Id = " + o.id + " ");
        s.append(o.dump(level, seenOpts));
      }
      s.append("<\\Parent>");
    }

    s.append(ls);
    s.append("<\\" + getName() + ">");
    return s.toString();
  }

  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator[] evals,
      int start, int length, ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] result = new ObjectInspector[length];
    for (int i = 0; i < length; i++) {
      result[i] = evals[start + i].initialize(rowInspector);
    }
    return result;
  }

  protected static ObjectInspector[] initEvaluators(ExprNodeEvaluator[] evals,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] result = new ObjectInspector[evals.length];
    for (int i = 0; i < evals.length; i++) {
      result[i] = evals[i].initialize(rowInspector);
    }
    return result;
  }

  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<String> outputColName,
      ObjectInspector rowInspector) throws HiveException {
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals,
        rowInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        outputColName, Arrays.asList(fieldObjectInspectors));
  }

}
