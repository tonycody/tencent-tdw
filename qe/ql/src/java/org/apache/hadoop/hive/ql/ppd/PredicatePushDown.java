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
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PredicatePushDown implements Transform {

  private ParseContext pGraphContext;
  private HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    this.pGraphContext = pctx;
    this.opToParseCtxMap = pGraphContext.getOpParseCtx();

    OpWalkerInfo opWalkerInfo = new OpWalkerInfo(opToParseCtxMap);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "FIL%"), OpProcFactory.getFilterProc());
    opRules.put(new RuleRegExp("R3", "JOIN%"), OpProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R4", "RS%"), OpProcFactory.getRSProc());
    opRules.put(new RuleRegExp("R5", "TS%"), OpProcFactory.getTSProc());
    opRules.put(new RuleRegExp("R6", "SCR%"), OpProcFactory.getSCRProc());
    opRules.put(new RuleRegExp("R6", "LIM%"), OpProcFactory.getLIMProc());
    opRules.put(new RuleRegExp("R6", "HSL%"), OpProcFactory.getHSLProc());
    opRules.put(new RuleRegExp("R6", "ANA%"), OpProcFactory.getAnaProc());
    opRules.put(new RuleRegExp("R7", "UDTF%"), OpProcFactory.getUDTFProc());
    opRules.put(new RuleRegExp("R8", "LVF%"), OpProcFactory.getLVFProc());

    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(),
        opRules, opWalkerInfo);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pGraphContext;
  }

}
