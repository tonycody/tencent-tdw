/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;

import org.antlr.runtime.CommonToken;

public class ExistsTranslator {

  private enum ExistsType {
    NO, EXISTS, NOTEXISTS
  };

  public void changeASTTreesForExists(ASTNode ast) throws SemanticException {

    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      ExistsCount existsCount = new ExistsCount();
      detectExists(ast, existsCount);

      ExistsType existsType = existsCount.type;

      if (existsCount.count >= 1) {
        translateMultiExists(ast);

      }
    }

    int child_count = ast.getChildCount();
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {

      changeASTTreesForExists((ASTNode) ast.getChild(child_pos));
    }
  }

  private class ExistsCount {
    ExistsType type;
    int count;
  }

  private ExistsType detectExists(ASTNode ast) throws SemanticException {

    for (int pos0 = 0; pos0 < ast.getChildCount(); ++pos0) {

      ASTNode level0 = (ASTNode) ast.getChild(pos0);
      if (level0.getToken().getType() == HiveParser.TOK_INSERT) {
        if (level0.getChildCount() < 3)
          return ExistsType.NO;

        ASTNode where = (ASTNode) level0.getChild(2);
        if (where.getToken().getType() == HiveParser.TOK_WHERE) {
          assert where.getChildCount() == 1;

          ASTNode whereChild = (ASTNode) where.getChild(0);

          if (whereChild.getToken().getText().equalsIgnoreCase("EXISTS")) {
            return ExistsType.EXISTS;

          } else if (whereChild.getToken().getText().equalsIgnoreCase("NOT")) {
            ASTNode grandson = (ASTNode) whereChild.getChild(0);
            if (grandson.getToken().getText().equalsIgnoreCase("EXISTS"))
              return ExistsType.NOTEXISTS;

          } else if (whereChild.getToken().getText().equalsIgnoreCase("AND")
              || whereChild.getToken().getText().equalsIgnoreCase("OR")) {
            ASTNode andOr = whereChild;
            Integer existsCnt = 0;
            ExistsType type = ExistsType.NO;
            ExistsCount existsCount = new ExistsCount();
            existsCount.count = 0;
            existsCount.type = ExistsType.NO;

            detestExistsInAndOr(andOr, existsCount);

            if (existsCount.count == 0)
              return ExistsType.NO;

            else if (existsCount.count > 1)
              throw new SemanticException("Too much Exists in the clause "
                  + where.toStringTree());

            return existsCount.type;

          } else {
            return ExistsType.NO;
          }

        } else {
          return ExistsType.NO;
        }
      }
    }

    return ExistsType.NO;
  }

  private void detectExists(ASTNode ast, ExistsCount existsCount)
      throws SemanticException {

    for (int pos0 = 0; pos0 < ast.getChildCount(); ++pos0) {

      ASTNode level0 = (ASTNode) ast.getChild(pos0);
      if (level0.getToken().getType() == HiveParser.TOK_INSERT) {
        if (level0.getChildCount() < 3) {
          existsCount.type = ExistsType.NO;
          existsCount.count = 0;
          return;
        }

        ASTNode where = (ASTNode) level0.getChild(2);
        if (where.getToken().getType() == HiveParser.TOK_WHERE) {
          assert where.getChildCount() == 1;

          ASTNode whereChild = (ASTNode) where.getChild(0);

          if (whereChild.getToken().getText().equalsIgnoreCase("EXISTS")) {
            existsCount.count = 1;
            existsCount.type = ExistsType.EXISTS;
            return;

          } else if (whereChild.getToken().getText().equalsIgnoreCase("NOT")) {
            ASTNode grandson = (ASTNode) whereChild.getChild(0);
            if (grandson.getToken().getText().equalsIgnoreCase("EXISTS")) {
              existsCount.count = 1;
              existsCount.type = ExistsType.NOTEXISTS;

              return;

            } else if (grandson.toStringTree().toLowerCase().contains("exists")) {
              throw new SemanticException(
                  "Not support EXISTS/NOT EXISTS in NOT expression!");
            }

          } else if (whereChild.getToken().getText().equalsIgnoreCase("AND")
              || whereChild.getToken().getText().equalsIgnoreCase("OR")) {
            ASTNode andOr = whereChild;

            detestExistsInAndOr(andOr, existsCount);

            return;

          } else {
            existsCount.count = 0;
            existsCount.type = ExistsType.NO;

            return;
          }

        } else {
          existsCount.count = 0;
          existsCount.type = ExistsType.NO;
          return;
        }
      }
    }

    existsCount.count = 0;
    existsCount.type = ExistsType.NO;

    return;
  }

  private void detestExistsInAndOr(ASTNode andOr, ExistsCount existCount)
      throws SemanticException {
    ASTNode child0 = (ASTNode) andOr.getChild(0);

    if (child0.getToken().getText().equalsIgnoreCase("EXISTS")) {
      existCount.type = ExistsType.EXISTS;
      existCount.count++;

    } else if (child0.getToken().getText().equalsIgnoreCase("NOT")) {
      ASTNode child00 = (ASTNode) child0.getChild(0);
      if (child00.getToken().getText().equalsIgnoreCase("EXISTS")) {
        existCount.type = ExistsType.NOTEXISTS;
        existCount.count++;
      } else if (child00.toStringTree().toLowerCase().contains("exists")) {
        throw new SemanticException(
            "Not support EXISTS/NOT EXISTS in NOT expression!");
      }

    } else if (child0.getToken().getText().equalsIgnoreCase("AND")
        || child0.getToken().getText().equalsIgnoreCase("OR")) {
      detestExistsInAndOr(child0, existCount);
    }

    ASTNode child1 = (ASTNode) andOr.getChild(1);
    if (child1.getToken().getText().equalsIgnoreCase("EXISTS")) {
      existCount.type = ExistsType.EXISTS;
      existCount.count++;

    } else if (child1.getToken().getText().equalsIgnoreCase("NOT")) {
      ASTNode child10 = (ASTNode) child1.getChild(0);
      if (child10.getToken().getText().equalsIgnoreCase("EXISTS")) {
        existCount.type = ExistsType.NOTEXISTS;
        existCount.count++;

      }

    } else if (child1.getToken().getText().equalsIgnoreCase("AND")
        || child1.getToken().getText().equalsIgnoreCase("OR")) {
      detestExistsInAndOr(child1, existCount);
    }
  }

  private void translateMultiExists(ASTNode query) throws SemanticException {

    MultiExistsTree tree = collectMultiExistsTreeInfo(query);
    String outerSource = tree.getOuterSource();
    ArrayList<String> innerSources = tree.getInnerSources();
    ArrayList<Boolean> isNotExists = tree.getIsNotExists();
    int existsCount = tree.getExistsCount();
    ArrayList<ASTNode> innerFroms = tree.getInnerFroms();

    ASTNode leftFrom = tree.getOuterFrom();
    ArrayList<ASTNode> existsCond = tree.getExistsCondition();
    ArrayList<Boolean> isJoinSources = tree.getIsJoinSources();
    boolean outerJoinSource = tree.getOuterJoinSource();

    for (Boolean isJS : isJoinSources) {
      if (isJS)
        throw new SemanticException(
            "Not support join in exists condition, Please rewrite the SQL!");
    }

    ArrayList<String> outerSourceList = new ArrayList<String>();
    if (tree.getOuterJoinSource()) {
      collectSourceNames(tree.getOuterFrom(), outerSourceList);
    } else {
      outerSourceList.add(outerSource);
    }

    ASTNode from = (ASTNode) query.getChild(0);
    assert from.getChildCount() == 1;
    from.deleteChild(0);
    ASTNode newFromChild = null;

    for (int i = 0; i < existsCount; i++) {
      if (isNotExists.get(i)) {
        newFromChild = new ASTNode(new CommonToken(
            HiveParser.TOK_LEFTOUTERJOIN, "TOK_LEFTOUTERJOIN"));
      } else
        newFromChild = new ASTNode(new CommonToken(HiveParser.TOK_LEFTSEMIJOIN,
            "TOK_LEFTSEMIJOIN"));

      newFromChild.addChild(leftFrom);
      newFromChild.addChild(innerFroms.get(i));

      ASTNode cond = existsCond.get(i);
      if (!outerJoinSource && !isJoinSources.get(i)) {
        addSource(cond, innerSources.get(i), outerSource);

      } else if (!isJoinSources.get(i)) {
        addSource(cond, innerSources.get(i), outerSourceList);
      }
      newFromChild.addChild(cond);

      leftFrom = newFromChild;
    }
    from.addChild(newFromChild);

    ASTNode insert = (ASTNode) query.getChild(1);
    for (int i = insert.getChildCount() - 1; i >= 0; i--)
      insert.deleteChild(i);

    insert.addChild(tree.getOuterDestination());

    if (tree.getIsSelectDI()) {
      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECTDI,
          "TOK_SELECTDI")));

    } else {
      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
          "TOK_SELECT")));
    }

    ASTNode select = (ASTNode) insert.getChild(1);
    int idx = 0;

    if (!outerJoinSource) {
      for (ASTNode selectNode : tree.getOuterSelects()) {

        addSource(selectNode, outerSource);
        select.addChild(selectNode);
      }

    } else {
      for (ASTNode selectNode : tree.getOuterSelects()) {
        select.addChild(selectNode);
      }
    }

    int whereCount = 0;
    idx = 0;
    ArrayList<ASTNode> wheres = new ArrayList<ASTNode>();
    if (tree.getOtherOuterWhere() != null) {
      if (!outerJoinSource) {
        for (ASTNode out : tree.getOtherOuterWhere()) {
          addSource(out, outerSource);
          wheres.add(out);
          whereCount++;
        }
      } else {
        for (ASTNode out : tree.getOtherOuterWhere()) {
          wheres.add(out);
          whereCount++;
        }
      }

    }

    ArrayList<ASTNode> nullList = new ArrayList<ASTNode>();

    ArrayList<String> innerSourceList = new ArrayList<String>();
    for (int i = 0; i < existsCount; i++) {
      if (isNotExists.get(i)) {
        innerSourceList.clear();
        ASTNode notExistsTree = existsCond.get(i);
        collectSourceNames(innerFroms.get(i), innerSourceList);

        getNullConditionForNotExists(notExistsTree, nullList, outerSourceList,
            innerSourceList);
      }
    }

    ASTNode nullFunc = null;
    for (ASTNode nullCond : nullList) {
      nullFunc = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION,
          "TOK_FUNCTION"));
      nullFunc.addChild(new ASTNode(new CommonToken(HiveParser.TOK_ISNULL,
          "TOK_ISNULL")));
      nullFunc.addChild(nullCond);

      wheres.add(nullFunc);
      whereCount++;
    }

    if (whereCount > 0) {
      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_WHERE,
          "TOK_WHERE")));

      ASTNode where = (ASTNode) insert.getChild(2);
      if (whereCount >= 2) {
        where.addChild(new ASTNode(new CommonToken(HiveParser.KW_AND, "AND")));
        ASTNode and = (ASTNode) where.getChild(0);

        while (whereCount > 2) {
          and.addChild(new ASTNode(new CommonToken(HiveParser.KW_AND, "AND")));
          and.addChild(wheres.get(idx++));
          whereCount--;
          and = (ASTNode) and.getChild(0);
        }

        and.addChild(wheres.get(idx++));
        whereCount--;
        and.addChild(wheres.get(idx++));
        whereCount--;
      } else if (whereCount == 1) {
        where.addChild(wheres.get(idx++));

      }
    }

    if (outerJoinSource) {

      ArrayList<ASTNode> nodes = tree.getOtherInsertNodes();
      if (nodes.size() > 0) {
        for (ASTNode node : nodes) {
          insert.addChild(node);
        }
      }

      if (query.getChildCount() > 2) {
        for (int i = 2; i < query.getChildCount(); i++) {
          ASTNode node = (ASTNode) query.getChild(i);
          query.addChild(node);
        }
      }

    } else {
      ArrayList<ASTNode> nodes = tree.getOtherInsertNodes();
      if (nodes.size() > 0) {
        for (ASTNode node : nodes) {
          addSource(node, outerSource);
          insert.addChild(node);
        }
      }

      if (query.getChildCount() > 2) {
        for (int i = 2; i < query.getChildCount(); i++) {
          ASTNode node = (ASTNode) query.getChild(i);
          addSource(node, outerSource);
          query.addChild(node);
        }
      }
    }
  }

  void collectSourceNames(ASTNode tree, ArrayList<String> sourceNames)
      throws SemanticException {

    if (tree.getToken().getType() == HiveParser.TOK_TABREF) {
      if (tree.getChildCount() == 2) {
        ASTNode child = (ASTNode) tree.getChild(1);
        sourceNames.add(child.getText().toLowerCase());
      } else {
        ASTNode child = (ASTNode) tree.getChild(0);
        ASTNode grandSon = (ASTNode) child.getChild(0);
        sourceNames.add(grandSon.getText().toLowerCase());
      }
      return;

    } else if (tree.getToken().getType() == HiveParser.TOK_SUBQUERY) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      if (tree.getChildCount() != 2) {
        throw new SemanticException("Wrong SubQuery, Need a Query Name! "
            + child0.toStringTree());
      }

      ASTNode child1 = (ASTNode) tree.getChild(1);
      sourceNames.add(child1.getText().toLowerCase());

      return;

    } else if (tree.getToken().getType() == HiveParser.TOK_JOIN
        || tree.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
        || tree.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
        || tree.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
        || tree.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
      if (tree.getChildCount() != 3)
        throw new SemanticException("Wrong Join Source: " + tree.toStringTree());

      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1 = (ASTNode) tree.getChild(1);

      collectSourceNames(child0, sourceNames);
      collectSourceNames(child1, sourceNames);

      return;

    } else
      throw new SemanticException("Not support this source in exists: "
          + tree.toStringTree());

  }

  void getNullConditionForNotExists(ASTNode notExistsTree,
      ArrayList<ASTNode> nullList, ArrayList<String> outerSourceList,
      ArrayList<String> innerSourceList) {

    if (notExistsTree.getToken().getText().equals("=")) {
      ASTNode child0 = (ASTNode) notExistsTree.getChild(0);
      ASTNode child1 = (ASTNode) notExistsTree.getChild(1);

      if ((findCol(child0, outerSourceList) && findCol(child1, innerSourceList))) {
        nullList.add(child1);
      } else if ((findCol(child1, outerSourceList) && findCol(child0,
          innerSourceList))) {
        nullList.add(child0);
      }

    } else if (notExistsTree.getToken().getText().equalsIgnoreCase("AND")) {
      ASTNode child0 = (ASTNode) notExistsTree.getChild(0);
      ASTNode child1 = (ASTNode) notExistsTree.getChild(1);

      getNullConditionForNotExists(child0, nullList, outerSourceList,
          innerSourceList);
      getNullConditionForNotExists(child1, nullList, outerSourceList,
          innerSourceList);
    }
  }

  private boolean findCol(ASTNode notExistTree, ArrayList<String> sourceList) {
    for (String innerSource : sourceList) {
      if (notExistTree.toStringTree().toLowerCase()
          .contains(". (tok_table_or_col " + innerSource.toLowerCase()))
        return true;
    }

    return false;
  }

  void findExistsCond(ASTNode exists, ArrayList<ASTNode> conds,
      String outerSource, String innerSource) {

    if (exists.getToken().getText().equals("=")) {
      ASTNode child0 = (ASTNode) exists.getChild(0);
      ASTNode child1 = (ASTNode) exists.getChild(1);

      if ((findInnerCol(child0, innerSource, outerSource) && findInnerCol(
          child1, outerSource, innerSource))
          || (findInnerCol(child1, innerSource, outerSource) && findInnerCol(
              child0, outerSource, innerSource))) {
        conds.add(exists);
      }

    } else if (exists.getToken().getText().equalsIgnoreCase("AND")) {
      findExistsCond((ASTNode) exists.getChild(0), conds, outerSource,
          innerSource);
      findExistsCond((ASTNode) exists.getChild(1), conds, outerSource,
          innerSource);
    }

  }

  private boolean findInnerCol(ASTNode exist, String innerSource,
      String outerSource) {
    if (exist.toStringTree().toLowerCase()
        .contains(". (tok_table_or_col " + innerSource.toLowerCase())) {
      return true;

    } else if (exist.toStringTree().toLowerCase()
        .contains(". (tok_table_or_col " + outerSource.toLowerCase())) {
      return false;
    }

    return false;
  }

  private void addSource(ASTNode node, String source, String otherSource) {
    int childCount = node.getChildCount();

    if (childCount > 0) {

      for (int i = 0; i < childCount; i++) {
        ASTNode childi = (ASTNode) node.getChild(i);

        if (childi.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
          if ((i + 1 == childCount)
              || !((node.getToken().getType() == HiveParser.DOT) && (((ASTNode) childi
                  .getChild(0)).getText().equalsIgnoreCase(source) || ((ASTNode) childi
                  .getChild(0)).getText().equalsIgnoreCase(otherSource)))) {

            node.setChild(i, new ASTNode(new CommonToken(HiveParser.DOT, ".")));

            ASTNode newNode = (ASTNode) node.getChild(i);
            newNode.addChild(childi);

            ASTNode col = (ASTNode) childi.getChild(0);
            newNode.addChild(col);

            childi.setChild(0, new ASTNode(new CommonToken(
                HiveParser.Identifier, source)));
          }

        } else if (childi.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          if (childi.getChildCount() == 0) {
            childi.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
                source)));
          }
        }
      }
    }

    for (int child_pos = 0; child_pos < childCount; ++child_pos) {
      addSource((ASTNode) node.getChild(child_pos), source, otherSource);
    }
  }

  private void addSource(ASTNode node, String source,
      ArrayList<String> otherSources) {
    int childCount = node.getChildCount();

    if (childCount > 0) {

      for (int i = 0; i < childCount; i++) {
        ASTNode childi = (ASTNode) node.getChild(i);

        if (childi.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
          if ((i + 1 == childCount)
              || !((node.getToken().getType() == HiveParser.DOT) && (((ASTNode) childi
                  .getChild(0)).getText().equalsIgnoreCase(source) || otherSources
                  .contains(((ASTNode) childi.getChild(0)).getText())))) {

            node.setChild(i, new ASTNode(new CommonToken(HiveParser.DOT, ".")));

            ASTNode newNode = (ASTNode) node.getChild(i);
            newNode.addChild(childi);

            ASTNode col = (ASTNode) childi.getChild(0);
            newNode.addChild(col);

            childi.setChild(0, new ASTNode(new CommonToken(
                HiveParser.Identifier, source)));
          }

        } else if (childi.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          if (childi.getChildCount() == 0) {
            childi.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
                source)));
          }
        }
      }
    }

    for (int child_pos = 0; child_pos < childCount; ++child_pos) {
      addSource((ASTNode) node.getChild(child_pos), source, otherSources);
    }
  }

  private void addSource(ASTNode node, String source) {
    int childCount = node.getChildCount();

    if (childCount > 0) {

      for (int i = 0; i < childCount; i++) {
        ASTNode childi = (ASTNode) node.getChild(i);

        if (childi.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
          if (i + 1 == childCount
              || !((node.getToken().getType() == HiveParser.DOT) && (((ASTNode) childi
                  .getChild(0)).getText().equalsIgnoreCase(source)))) {

            node.setChild(i, new ASTNode(new CommonToken(HiveParser.DOT, ".")));

            ASTNode newNode = (ASTNode) node.getChild(i);
            newNode.addChild(childi);

            ASTNode col = (ASTNode) childi.getChild(0);
            newNode.addChild(col);

            childi.setChild(0, new ASTNode(new CommonToken(
                HiveParser.Identifier, source)));
          }

        } else if (childi.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          if (childi.getChildCount() == 0) {
            childi.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
                source)));
          }
        }
      }
    }

    for (int child_pos = 0; child_pos < childCount; ++child_pos) {
      addSource((ASTNode) node.getChild(child_pos), source);
    }
  }

  private ExistsTree collectExistsTreeInfo(ASTNode query)
      throws SemanticException {
    ExistsTree tree = new ExistsTree();
    String outerSource;

    ASTNode from = (ASTNode) query.getChild(0);
    if (from.getToken().getType() == HiveParser.TOK_FROM) {
      if (from.getChildCount() != 1)
        throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

      ASTNode child0 = (ASTNode) from.getChild(0);

      if (child0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        if (child0.getChildCount() == 2) {
          outerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setOuterSource(outerSource);

          tree.setOuterFrom(child0);
        } else
          throw new SemanticException(
              "An alias must be provided for the query: "
                  + child0.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_TABREF) {

        if (child0.getChildCount() == 2) {
          outerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setOuterSource(outerSource);

          tree.setOuterFrom(child0);

        } else if (child0.getChildCount() == 1) {
          ASTNode child00 = (ASTNode) child0.getChild(0);
          if (child00.getToken().getType() == HiveParser.TOK_TAB) {
            outerSource = ((ASTNode) child00.getChild(0)).getText();
            tree.setOuterSource(outerSource);

            tree.setOuterFrom(child0);
          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + query.toStringTree());

        } else
          throw new SemanticException("Wrong EXISTS SQL: "
              + query.toStringTree());

      } else
        throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

    } else
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

    ASTNode insert = (ASTNode) query.getChild(1);
    if (insert.getChildCount() < 3)
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

    ASTNode outerDestination = (ASTNode) insert.getChild(0);
    if ((outerDestination.getToken().getType() != HiveParser.TOK_DESTINATION)
        && (outerDestination.getToken().getType() != HiveParser.TOK_APPENDDESTINATION))
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());
    tree.setOuterDestination(outerDestination);

    ASTNode outerSelects = (ASTNode) insert.getChild(1);
    if (outerSelects.getToken().getType() != HiveParser.TOK_SELECT)
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

    for (int i = 0; i < outerSelects.getChildCount(); i++) {
      tree.setOuterSelect((ASTNode) outerSelects.getChild(i));
    }

    ASTNode outerWhere = (ASTNode) insert.getChild(2);
    if (outerWhere.getToken().getType() != HiveParser.TOK_WHERE)
      throw new SemanticException("Wrong EXISTS SQL: "
          + outerWhere.toStringTree());
    if (outerWhere.getChildCount() == 1) {
      ASTNode child0 = (ASTNode) outerWhere.getChild(0);

      if (child0.getText().equalsIgnoreCase("AND")) {
        collectOuterAndInfo(child0, tree);

      } else if (child0.getText().equalsIgnoreCase("OR")) {
        throw new SemanticException("Not support EXISTS in the OR clause!");

      } else {
        if (child0.getText().equalsIgnoreCase("EXISTS")) {
          tree.setNotExists(false);
          collectExistsInfo(child0, tree);

        } else if (child0.getText().equalsIgnoreCase("NOT")) {
          ASTNode child00 = (ASTNode) child0.getChild(0);

          if (child00.getText().equalsIgnoreCase("EXISTS")) {
            tree.setNotExists(true);
            collectExistsInfo(child00, tree);

          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + child00.toStringTree());

        } else {
          throw new SemanticException("Wrong EXISTS SQL: "
              + child0.toStringTree());
        }
      }

    } else {
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());
    }

    if (insert.getChildCount() >= 4) {
      for (int i = 3; i < insert.getChildCount(); i++) {
        tree.setOtherInsertNodes((ASTNode) insert.getChild(i));
      }
    }

    if (query.getChildCount() > 2) {
      for (int i = 2; i < query.getChildCount(); i++) {
        tree.setOtherNodes((ASTNode) query.getChild(i));
      }
    }

    return tree;
  }

  private MultiExistsTree collectMultiExistsTreeInfo(ASTNode query)
      throws SemanticException {
    MultiExistsTree tree = new MultiExistsTree();
    String outerSource;

    ASTNode from = (ASTNode) query.getChild(0);
    if (from.getToken().getType() == HiveParser.TOK_FROM) {
      if (from.getChildCount() != 1)
        throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

      ASTNode child0 = (ASTNode) from.getChild(0);

      if (child0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        if (child0.getChildCount() == 2) {
          outerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setOuterSource(outerSource);

          tree.setOuterFrom(child0);
        } else
          throw new SemanticException(
              "An alias must be provided for the query: "
                  + child0.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_TABREF) {

        if (child0.getChildCount() == 2) {
          outerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setOuterSource(outerSource);

          tree.setOuterFrom(child0);

        } else if (child0.getChildCount() == 1) {
          ASTNode child00 = (ASTNode) child0.getChild(0);
          if (child00.getToken().getType() == HiveParser.TOK_TAB) {
            outerSource = ((ASTNode) child00.getChild(0)).getText();
            tree.setOuterSource(outerSource);

            tree.setOuterFrom(child0);
          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + query.toStringTree());

        } else
          throw new SemanticException("Wrong EXISTS SQL: "
              + query.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_JOIN
          || child0.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
        outerSource = "joinsource";

        tree.setOuterJoinSource(true);
        tree.setOuterSource(outerSource);
        tree.setOuterFrom(child0);

      } else
        throw new SemanticException("Wrong EXISTS SQL With Wrong Source: "
            + query.toStringTree());

    } else
      throw new SemanticException("Wrong EXISTS SQL With Wrong Source: "
          + query.toStringTree());

    ASTNode insert = (ASTNode) query.getChild(1);
    if (insert.getChildCount() < 3)
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());

    ASTNode outerDestination = (ASTNode) insert.getChild(0);
    if ((outerDestination.getToken().getType() != HiveParser.TOK_DESTINATION)
        && (outerDestination.getToken().getType() != HiveParser.TOK_APPENDDESTINATION))
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());
    tree.setOuterDestination(outerDestination);

    ASTNode outerSelects = (ASTNode) insert.getChild(1);
    if (outerSelects.getToken().getType() == HiveParser.TOK_SELECT) {
      tree.setIsSelectDI(false);
    } else if (outerSelects.getToken().getType() == HiveParser.TOK_SELECTDI) {
      tree.setIsSelectDI(true);
    } else {
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());
    }

    for (int i = 0; i < outerSelects.getChildCount(); i++) {
      tree.setOuterSelect((ASTNode) outerSelects.getChild(i));
    }

    ASTNode outerWhere = (ASTNode) insert.getChild(2);
    if (outerWhere.getToken().getType() != HiveParser.TOK_WHERE)
      throw new SemanticException("Wrong EXISTS SQL: "
          + outerWhere.toStringTree());
    if (outerWhere.getChildCount() == 1) {
      ASTNode child0 = (ASTNode) outerWhere.getChild(0);

      if (child0.getText().equalsIgnoreCase("AND")) {
        collectOuterAndInfo(child0, tree);

      } else if (child0.getText().equalsIgnoreCase("OR")) {
        throw new SemanticException("Not support EXISTS in the OR clause!");

      } else {
        if (child0.getText().equalsIgnoreCase("EXISTS")) {
          tree.setNotExists(false);
          collectExistsInfo(child0, tree);

        } else if (child0.getText().equalsIgnoreCase("NOT")) {
          ASTNode child00 = (ASTNode) child0.getChild(0);

          if (child00.getText().equalsIgnoreCase("EXISTS")) {
            tree.setNotExists(true);
            collectExistsInfo(child00, tree);

          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + child00.toStringTree());

        } else {
          throw new SemanticException("Wrong EXISTS SQL: "
              + child0.toStringTree());
        }
      }

    } else {
      throw new SemanticException("Wrong EXISTS SQL: " + query.toStringTree());
    }

    if (insert.getChildCount() >= 4) {
      for (int i = 3; i < insert.getChildCount(); i++) {
        tree.setOtherInsertNodes((ASTNode) insert.getChild(i));
      }
    }

    if (query.getChildCount() > 2) {
      for (int i = 2; i < query.getChildCount(); i++) {
        tree.setOtherNodes((ASTNode) query.getChild(i));
      }
    }

    return tree;
  }

  private void collectOuterAndInfo(ASTNode and, ExistsTree tree)
      throws SemanticException {

    assert and.getChildCount() == 2;

    for (int i = 0; i < and.getChildCount(); i++) {
      ASTNode childi = (ASTNode) and.getChild(i);

      if (childi.getText().equalsIgnoreCase("AND")) {
        collectOuterAndInfo(childi, tree);

      } else if (childi.getText().equalsIgnoreCase("EXISTS")) {
        tree.setNotExists(false);
        collectExistsInfo(childi, tree);

      } else if (childi.getText().equalsIgnoreCase("NOT")) {
        ASTNode childi0 = (ASTNode) childi.getChild(0);

        if (childi0.getText().equalsIgnoreCase("EXISTS")) {
          tree.setNotExists(true);
          collectExistsInfo(childi0, tree);
        } else
          tree.setOtherOuterWhere(childi);

      } else if (childi.getText().equalsIgnoreCase("OR")) {
        if (childi.toStringTree().toLowerCase()
            .contains("exists (tok_subquery (tok_query"))
          throw new SemanticException("Not support EXISTS in the OR clause!");
        else
          tree.setOtherOuterWhere(childi);

      } else {
        tree.setOtherOuterWhere(childi);
      }
    }
  }

  private void collectOuterAndInfo(ASTNode and, MultiExistsTree tree)
      throws SemanticException {

    assert and.getChildCount() == 2;

    for (int i = 0; i < and.getChildCount(); i++) {
      ASTNode childi = (ASTNode) and.getChild(i);

      if (childi.getText().equalsIgnoreCase("AND")) {
        collectOuterAndInfo(childi, tree);

      } else if (childi.getText().equalsIgnoreCase("EXISTS")) {
        tree.setNotExists(false);
        collectExistsInfo(childi, tree);

      } else if (childi.getText().equalsIgnoreCase("NOT")) {
        ASTNode childi0 = (ASTNode) childi.getChild(0);

        if (childi0.getText().equalsIgnoreCase("EXISTS")) {
          tree.setNotExists(true);
          collectExistsInfo(childi0, tree);
        } else
          tree.setOtherOuterWhere(childi);

      } else if (childi.getText().equalsIgnoreCase("OR")) {
        if (childi.toStringTree().toLowerCase()
            .contains("exists (tok_subquery (tok_query"))
          throw new SemanticException("Not support EXISTS in the OR clause!");
        else
          tree.setOtherOuterWhere(childi);

      } else {
        tree.setOtherOuterWhere(childi);
      }
    }
  }

  private void collectInnerAndInfo(ASTNode and, ExistsTree tree,
      String outerSource) throws SemanticException {

    assert and.getChildCount() == 2;

    for (int i = 0; i < and.getChildCount(); i++) {
      ASTNode childi = (ASTNode) and.getChild(i);

      if (childi.getText().equalsIgnoreCase("AND")) {
        collectInnerAndInfo(childi, tree, outerSource);

      } else if (childi.getText().equalsIgnoreCase("EXISTS")) {
        throw new SemanticException(
            "Not support EXISTS in the inner sql query!");

      } else if (childi.getText().equalsIgnoreCase("NOT")) {
        ASTNode childi0 = (ASTNode) childi.getChild(0);

        if (childi0.getText().equalsIgnoreCase("EXISTS")) {
          throw new SemanticException(
              "Not support EXISTS in the inner sql query!");
        } else
          tree.setOtherInnerWhere(childi);

      } else if (childi.getText().equalsIgnoreCase("OR")) {
        if (childi.toStringTree().toLowerCase()
            .contains("exists (tok_subquery (tok_query"))
          throw new SemanticException(
              "Not support EXISTS in the inner sql query!");
        else
          tree.setOtherInnerWhere(childi);

      } else {
        if (childi.toStringTree().contains("TOK_TABLE_OR_COL " + outerSource))
          tree.setExistsCondition(childi);
        else
          tree.setOtherInnerWhere(childi);
      }
    }
  }

  private void collectExistsInfo(ASTNode exists, ExistsTree tree)
      throws SemanticException {
    ASTNode childTree = (ASTNode) ((ASTNode) exists.getChild(0)).getChild(0);
    String innerSource;
    String outerSource = tree.getOuterSource();

    if (childTree.getChildCount() > 2)
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");

    assert childTree.getChildCount() == 2;

    ASTNode from = (ASTNode) childTree.getChild(0);
    if (from.getToken().getType() == HiveParser.TOK_FROM) {
      if (from.getChildCount() != 1)
        throw new SemanticException("Wrong EXISTS SQL: "
            + childTree.toStringTree());

      ASTNode child0 = (ASTNode) from.getChild(0);
      if (child0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        if (child0.getChildCount() == 2) {
          innerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setInnerSource(innerSource);

          tree.setInnerFrom(child0);
        } else
          throw new SemanticException(
              "An alias must be provided for the query: "
                  + child0.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_TABREF) {
        if (child0.getChildCount() == 2) {
          innerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();
          tree.setInnerSource(innerSource);

          tree.setInnerFrom(child0);

        } else if (child0.getChildCount() == 1) {
          ASTNode child00 = (ASTNode) child0.getChild(0);
          if (child00.getToken().getType() == HiveParser.TOK_TAB) {
            innerSource = ((ASTNode) child00.getChild(0)).getText();
            tree.setInnerSource(innerSource);

            tree.setInnerFrom(child0);
          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + childTree.toStringTree());

        } else
          throw new SemanticException("Wrong EXISTS SQL: "
              + childTree.toStringTree());

      } else
        throw new SemanticException("Wrong EXISTS SQL: "
            + childTree.toStringTree());

    } else
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());

    ASTNode insert = (ASTNode) childTree.getChild(1);

    ASTNode innerDestination = (ASTNode) insert.getChild(0);
    if ((innerDestination.getToken().getType() != HiveParser.TOK_DESTINATION)
        && (innerDestination.getToken().getType() != HiveParser.TOK_APPENDDESTINATION))
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());

    ASTNode innerSelects = (ASTNode) insert.getChild(1);
    if (innerSelects.getToken().getType() != HiveParser.TOK_SELECT)
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());
    for (int i = 0; i < innerSelects.getChildCount(); i++) {
      tree.setInnerSelect((ASTNode) innerSelects.getChild(i));
    }

    if (insert.getChildCount() > 3)
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");

    ASTNode innerWhere = (ASTNode) insert.getChild(2);
    if (innerWhere.getToken().getType() != HiveParser.TOK_WHERE)
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());
    ASTNode where0 = (ASTNode) innerWhere.getChild(0);
    if (where0.getText().equals("=")) {
      if (where0.toStringTree().contains("TOK_TABLE_OR_COL " + outerSource))
        tree.setExistsCondition(where0);

      else
        throw new SemanticException("Wrong EXISTS SQL: "
            + childTree.toStringTree());

    } else if (where0.getText().equalsIgnoreCase("AND")) {
      if (!testBadExistsCondition(where0, outerSource, innerSource)) {
        tree.setExistsCondition(where0);

      } else {
        throw new SemanticException(
            "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");
      }

    } else {
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");
    }

  }

  private void collectExistsInfo(ASTNode exists, MultiExistsTree tree)
      throws SemanticException {
    ASTNode childTree = (ASTNode) ((ASTNode) exists.getChild(0)).getChild(0);
    String innerSource;
    String outerSource = tree.getOuterSource();

    if (childTree.getChildCount() > 2)
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");

    ASTNode from = (ASTNode) childTree.getChild(0);
    boolean isJoinSource = false;
    if (from.getToken().getType() == HiveParser.TOK_FROM) {
      if (from.getChildCount() != 1)
        throw new SemanticException("Wrong EXISTS SQL: "
            + childTree.toStringTree());

      ASTNode child0 = (ASTNode) from.getChild(0);
      if (child0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        if (child0.getChildCount() == 2) {
          innerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();

          tree.setInnerSource(innerSource);
          tree.setIsJoinSource(isJoinSource);
          tree.setInnerFrom(child0);
        } else
          throw new SemanticException(
              "An alias must be provided for the query: "
                  + child0.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_TABREF) {
        if (child0.getChildCount() == 2) {
          innerSource = ((ASTNode) child0.getChild(1)).getText().toLowerCase();

          tree.setInnerSource(innerSource);
          tree.setIsJoinSource(isJoinSource);
          tree.setInnerFrom(child0);

        } else if (child0.getChildCount() == 1) {
          ASTNode child00 = (ASTNode) child0.getChild(0);
          if (child00.getToken().getType() == HiveParser.TOK_TAB) {
            innerSource = ((ASTNode) child00.getChild(0)).getText();

            tree.setInnerSource(innerSource);
            tree.setIsJoinSource(isJoinSource);
            tree.setInnerFrom(child0);
          } else
            throw new SemanticException("Wrong EXISTS SQL: "
                + childTree.toStringTree());

        } else
          throw new SemanticException("Wrong EXISTS SQL: "
              + childTree.toStringTree());

      } else if (child0.getToken().getType() == HiveParser.TOK_JOIN
          || child0.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN
          || child0.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN) {
        innerSource = "joinsource";

        isJoinSource = true;
        tree.setInnerSource(innerSource);
        tree.setIsJoinSource(isJoinSource);
        tree.setInnerFrom(child0);
      } else
        throw new SemanticException("Wrong EXISTS SQL: "
            + childTree.toStringTree());

    } else
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());

    ASTNode insert = (ASTNode) childTree.getChild(1);

    ASTNode innerDestination = (ASTNode) insert.getChild(0);
    if ((innerDestination.getToken().getType() != HiveParser.TOK_DESTINATION)
        && (innerDestination.getToken().getType() != HiveParser.TOK_APPENDDESTINATION))
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());

    ASTNode innerSelects = (ASTNode) insert.getChild(1);
    if (innerSelects.getToken().getType() != HiveParser.TOK_SELECT)
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());
    ArrayList<ASTNode> selects = new ArrayList<ASTNode>();
    for (int i = 0; i < innerSelects.getChildCount(); i++) {
      selects.add((ASTNode) innerSelects.getChild(i));
    }
    tree.setInnerSelect(selects);

    if (insert.getChildCount() > 3)
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");

    ASTNode innerWhere = (ASTNode) insert.getChild(2);
    if (innerWhere.getToken().getType() != HiveParser.TOK_WHERE)
      throw new SemanticException("Wrong EXISTS SQL: "
          + childTree.toStringTree());
    ASTNode where0 = (ASTNode) innerWhere.getChild(0);
    if (where0.getText().equals("=")) {
      if (!isJoinSource && !tree.getOuterJoinSource()) {
        if (where0.toStringTree().toLowerCase()
            .contains("tok_table_or_col " + outerSource.toLowerCase())
            && where0.toStringTree().toLowerCase()
                .contains("tok_table_or_col " + innerSource.toLowerCase()))
          tree.setExistsCondition(where0);

        else
          throw new SemanticException("Wrong EXISTS SQL: "
              + childTree.toStringTree());

      } else if (!isJoinSource) {
        if (where0.toStringTree().toLowerCase()
            .contains("tok_table_or_col " + innerSource.toLowerCase()))
          tree.setExistsCondition(where0);

        else
          throw new SemanticException("Wrong EXISTS SQL: "
              + childTree.toStringTree());

      } else if (!tree.getOuterJoinSource()) {
        if (where0.toStringTree().toLowerCase()
            .contains("tok_table_or_col " + outerSource.toLowerCase()))
          tree.setExistsCondition(where0);

        else
          throw new SemanticException("Wrong EXISTS SQL: "
              + childTree.toStringTree());

      } else {
        tree.setExistsCondition(where0);
      }

    } else if (where0.getText().equalsIgnoreCase("AND")) {
      if (!testBadExistsCondition(where0, outerSource, innerSource)) {
        tree.setExistsCondition(where0);

      } else {
        throw new SemanticException(
            "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");
      }

    } else {
      throw new SemanticException(
          "Please rewrite the EXISTS SQL with Another Inner SQL to simplify the EXISTS SQL!");
    }

  }

  private boolean testBadExistsCondition(ASTNode joinCond, String outerSource,
      String innerSource) {
    assert joinCond.getChildCount() == 2;
    boolean result = false;

    for (int i = 0; i < joinCond.getChildCount(); i++) {
      ASTNode childi = (ASTNode) joinCond.getChild(i);

      if (childi.getText().equalsIgnoreCase("AND")) {
        result = result
            | testBadExistsCondition(childi, outerSource, innerSource);

      } else if (childi.getText().equalsIgnoreCase("OR")) {
        result = true;

      } else {
        result = false | result;
      }
    }

    return result;
  }
}
