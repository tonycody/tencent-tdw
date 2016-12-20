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

package org.apache.hadoop.hive.ql.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LineageInfo implements NodeProcessor {

  TreeSet<String> inputTableList = new TreeSet<String>();

  TreeSet<String> OutputTableList = new TreeSet<String>();

  public TreeSet<String> getInputTableList() {
    return inputTableList;
  }

  public TreeSet<String> getOutputTableList() {
    return OutputTableList;
  }

  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    ASTNode pt = (ASTNode) nd;

    switch (pt.getToken().getType()) {

    case HiveParser.TOK_TAB:
      OutputTableList.add(pt.getChild(0).getText());
      break;

    case HiveParser.TOK_TABREF:
      String table_name = ((ASTNode) pt.getChild(0).getChild(0)).getText();
      inputTableList.add(table_name);
      break;
    }
    return null;
  }

  public void getLineageInfo(String query) throws ParseException,
      SemanticException {

    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(query);

    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (ASTNode) tree.getChild(0);
    }

    inputTableList.clear();
    OutputTableList.clear();

    Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

    Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(tree);
    ogw.startWalking(topNodes, null);

    OutputTableList.removeAll(inputTableList);
  }

  public static void main(String[] args) throws IOException, ParseException,
      SemanticException {

    String query = args[0];

    LineageInfo lep = new LineageInfo();

    lep.getLineageInfo(query);

    for (String tab : lep.getInputTableList()) {
      System.out.println("InputTable=" + tab);
    }

    for (String tab : lep.getOutputTableList()) {
      System.out.println("OutputTable=" + tab);
    }
  }
}
