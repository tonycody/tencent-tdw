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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class JoinReorder implements Transform {

  private int getOutputSize(Operator<? extends Serializable> operator,
      Set<String> bigTables) {
    if (operator instanceof JoinOperator) {
      for (Operator<? extends Serializable> o : operator.getParentOperators()) {
        if (getOutputSize(o, bigTables) != 0) {
          return 1;
        }
      }
    }

    if (operator instanceof TableScanOperator) {
      String alias = ((TableScanOperator) operator).getConf().getAlias();
      if (bigTables.contains(alias)) {
        return 2;
      }
    }

    int maxSize = 0;
    if (operator.getParentOperators() != null) {
      for (Operator<? extends Serializable> o : operator.getParentOperators()) {
        int current = getOutputSize(o, bigTables);
        if (current > maxSize) {
          maxSize = current;
        }
      }
    }

    return maxSize;
  }

  private Set<String> getBigTables(ParseContext joinCtx) {
    Set<String> bigTables = new HashSet<String>();

    for (QBJoinTree qbJoin : joinCtx.getJoinContext().values()) {
      if (qbJoin.getStreamAliases() != null) {
        bigTables.addAll(qbJoin.getStreamAliases());
      }
    }

    return bigTables;
  }

  private void reorder(JoinOperator joinOp, Set<String> bigTables) {
    int count = joinOp.getParentOperators().size();

    int biggestPos = count - 1;
    int biggestSize = getOutputSize(
        joinOp.getParentOperators().get(biggestPos), bigTables);
    for (int i = 0; i < count - 1; i++) {
      int currSize = getOutputSize(joinOp.getParentOperators().get(i),
          bigTables);
      if (currSize > biggestSize) {
        biggestSize = currSize;
        biggestPos = i;
      }
    }

    Byte[] tagOrder = joinOp.getConf().getTagOrder();
    Byte temp = tagOrder[biggestPos];
    tagOrder[biggestPos] = tagOrder[count - 1];
    tagOrder[count - 1] = temp;

    ((ReduceSinkOperator) joinOp.getParentOperators().get(biggestPos))
        .getConf().setTag(count - 1);
    ((ReduceSinkOperator) joinOp.getParentOperators().get(count - 1)).getConf()
        .setTag(biggestPos);
  }

  public ParseContext transform(ParseContext pactx) throws SemanticException {
    Set<String> bigTables = getBigTables(pactx);

    for (JoinOperator joinOp : pactx.getJoinContext().keySet()) {
      reorder(joinOp, bigTables);
    }

    return pactx;
  }
}
