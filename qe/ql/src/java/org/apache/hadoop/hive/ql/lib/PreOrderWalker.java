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

package org.apache.hadoop.hive.ql.lib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.lang.Object;

import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PreOrderWalker extends DefaultGraphWalker {

  public PreOrderWalker(Dispatcher disp) {
    super(disp);
  }

  public void walk(Node nd) throws SemanticException {
    opStack.push(nd);
    dispatch(nd, opStack);

    if (nd.getChildren() != null)
      for (Node n : nd.getChildren())
        walk(n);

    opStack.pop();
  }
}
