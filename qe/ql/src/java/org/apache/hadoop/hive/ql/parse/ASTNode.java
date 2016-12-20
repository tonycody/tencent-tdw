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

package org.apache.hadoop.hive.ql.parse;

import java.util.Vector;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.lib.Node;

import org.antlr.runtime.CommonToken;

public class ASTNode extends CommonTree implements Node {

  private ASTNodeOrigin origin;

  public ASTNode() {
  }

  public ASTNode(Token t) {
    super(t);
  }

  public static ASTNode get(int type, String name) {

    CommonToken tok = new CommonToken(type);
    tok.setText(name);
    return new ASTNode(tok);
  }

  public ASTNode dupNode() {
    return new ASTNode(this.getToken());
  }

  public ASTNode repNode() {
    ASTNode re = this.dupNode();
    if (this.getChildCount() == 0) {
      return re;
    } else {
      for (int i = 0; i < this.getChildCount(); i++) {
        re.addChild(((ASTNode) this.getChild(i)).repNode());
      }
      return re;
    }
  }

  public Vector<Node> getChildren() {
    if (super.getChildCount() == 0) {
      return null;
    }

    Vector<Node> ret_vec = new Vector<Node>();
    for (int i = 0; i < super.getChildCount(); ++i) {
      ret_vec.add((Node) super.getChild(i));
    }

    return ret_vec;
  }

  public String getName() {
    return (new Integer(super.getToken().getType())).toString();
  }

  public ASTNodeOrigin getOrigin() {
    return origin;
  }

  public void setOrigin(ASTNodeOrigin origin) {
    this.origin = origin;
  }

  public String dump() {
    StringBuffer sb = new StringBuffer();

    sb.append('(');
    sb.append(this.toString());
    Vector<Node> children = getChildren();
    if (children != null) {
      for (Node node : getChildren()) {
        if (node instanceof ASTNode) {
          sb.append(((ASTNode) node).dump());
        } else {
          sb.append("NON-ASTNODE!!");
        }
      }
    }
    sb.append(')');
    return sb.toString();
  }
}
