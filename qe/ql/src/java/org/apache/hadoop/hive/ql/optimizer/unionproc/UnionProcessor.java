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

package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.optimizer.Transform;

public class UnionProcessor implements Transform {

  public UnionProcessor() {
  }

  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R1"), "RS%.*UNION%"),
        UnionProcFactory.getMapRedUnion());
    opRules.put(new RuleRegExp(new String("R2"), "UNION%.*UNION%"),
        UnionProcFactory.getUnknownUnion());
    opRules.put(new RuleRegExp(new String("R3"), "TS%.*UNION%"),
        UnionProcFactory.getMapUnion());
    opRules.put(new RuleRegExp(new String("R3"), "MAPJOIN%.*UNION%"),
        UnionProcFactory.getMapJoinUnion());
    UnionProcContext uCtx = new UnionProcContext();
    Dispatcher disp = new DefaultRuleDispatcher(UnionProcFactory.getNoUnion(),
        opRules, uCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pCtx.setUCtx(uCtx);

    return pCtx;
  }
}
